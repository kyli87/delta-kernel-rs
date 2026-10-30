//! Validates staged file actions for a declarative transaction.
//!
//! The main components of this module are:
//!
//! 1. A shared plan builder (`build_plan`) which unions all staged inputs and feeds them to a set
//!    of checks.
//! 2. A row-level check framework (see `StagedDataCheck` trait) whose checks read the shared input
//!    schema and project errors into fixed result slots declared in `schemas`.
//! 3. A shared validation function (see `validate_result`) which validates the result of the plan
//!    against the set of checks.
//!
//! ## Core Plan Shape
//!
//! The overall pre-commit validation plan has the following shape:
//!
//! 1. Normalize staged adds and removes to the common input fields plus the table's statistics
//!    subtree, then UNION them into a single input stream.
//! 2. PROJECT the input stream into rows containing a single column for each check (dictated by the
//!    check's `project_error` expression).
//! 3. AGGREGATE into a single row containing a single column for each check (taking the
//!    FIRST_NON_NULL value for each check).
//! 4. PROJECT the aggregate row into two columns per check:
//!    - a non-null `is_valid` Boolean indicating whether the check passed or failed
//!    - a nullable error struct (declared in `schemas`) containing error information for the failed
//!      check
//!
//! The single-row output of the plan is then visited by the `validate_result` function, which
//! requests all result columns in one visit and returns an error with the first failing check.
//! Disabled checks retain their result slots with `is_valid = true` and a null error struct.

mod checks;
mod schemas;

use std::sync::Arc;

pub(super) use checks::{
    AddFileFieldsCheck, AppendOnlyCheck, ClusteringStatsCheck, NumRecordsCheck,
    RemoveFileFieldsCheck,
};
use schemas::{CheckKind, ERROR_SCHEMA, INPUT_SCHEMA, RESULT_COLUMNS, RESULT_SCHEMA};

use crate::engine_data::{GetData, RowVisitor};
use crate::expressions::{ColumnName, Expression, Predicate, Scalar, StructData, null_lit};
use crate::plans::ir::nodes::{Agg, Aggregate};
use crate::plans::{Plan, PlanBuilder, RelationRef};
use crate::schema::{DataType, StructField, StructType};
use crate::struct_patch::StructPatchBuilder;
use crate::{EngineData, KernelError, KernelResult, KernelResultIterator};

/// Checks selected from the transaction's effective configuration, in validation order.
pub(super) type CheckList = Vec<Box<dyn StagedDataCheck>>;

/// Defines inputs, outputs, and error expression for performing a single, row-level, pre-commit
/// check.
///
/// Each check projects a null error struct if the check passed, or a present error struct if the
/// check failed. Inputs come from `schemas::INPUT_SCHEMA` plus the table's statistics subtree;
/// error fields must match the fixed schema for the check's `kind`.
pub(super) trait StagedDataCheck {
    /// Identifies this check's slot in the shared result schema.
    fn kind(&self) -> CheckKind;
    /// Returns the fixed name used to prefix this check's result columns.
    fn name(&self) -> &'static str {
        self.kind().name()
    }
    /// Returns an expression which projects a null error struct if the check passed, or a present
    /// error struct if the check failed (output must match the schema for `kind`).
    fn project_error(&self) -> Expression;
    /// Constructs an error from a non-null error struct containing the projected fields from the
    /// failing row.
    fn build_error(&self, sample: &StructData) -> KernelResult<KernelError>;
}

/// Builds a declarative plan that resolves down to a single-row summary containing results from the
/// given `checks`. See the module documentation for more details on the plan shape.
/// `stats_schema` comes from the transaction's table configuration, independently of the checks.
pub(super) fn build_plan(
    adds: &[RelationRef],
    removes: &[RelationRef],
    checks: &[Box<dyn StagedDataCheck>],
    stats_schema: &StructType,
) -> KernelResult<Plan> {
    if checks.is_empty() {
        return Err(KernelError::internal_error(
            "Pre-commit validation requires checks",
        ));
    }
    for (index, check) in checks.iter().enumerate() {
        if checks[..index]
            .iter()
            .any(|other| other.kind() == check.kind())
        {
            return Err(KernelError::internal_error(format!(
                "Duplicate pre-commit check '{}'",
                check.name(),
            )));
        }
    }
    let input = build_input(adds, removes, stats_schema)?;

    // Project the input with each of the check's error expressions.
    // The resulting schema is a struct with one field per check, each field named as
    // "<check_name>.err"
    let expressions = CheckKind::ALL.map(|kind| {
        checks
            .iter()
            .find(|check| check.kind() == kind)
            .map(|check| check.project_error())
            .unwrap_or_else(|| null_lit(kind.error_schema().clone()))
    });
    let input = input.project(Expression::struct_from(expressions), ERROR_SCHEMA.clone())?;

    // Aggregate the results into a single row containing a single column for each check (taking the
    // FIRST_NON_NULL value for each check).
    let mut aggregate = Aggregate::ungrouped(ERROR_SCHEMA.clone());
    for kind in CheckKind::ALL {
        let err_name = kind.error_name();
        aggregate =
            aggregate.aggregate_as(Agg::first_non_null(ColumnName::new([&err_name])), &err_name);
    }

    // Project the aggregate row into one `<check_name>.is_valid` and one `<check_name>.err` column
    // per check.
    let summary_exprs = CheckKind::ALL.into_iter().flat_map(|kind| {
        let error = Expression::column([kind.error_name()]);
        [Predicate::is_null(error.clone()).into(), error]
    });
    input
        .aggregate(aggregate)?
        .project(
            Expression::struct_from(summary_exprs),
            RESULT_SCHEMA.clone(),
        )?
        .build()
}

/// Validates the resolved output of running the pre-commit validation plan (from `build_plan`)
/// using the given `checks`.
/// Empty batches are ignored, but the stream must contain exactly one row in total. Stream errors
/// are propagated even when they occur after the summary row.
pub(super) fn validate_result(
    results: impl KernelResultIterator<Box<dyn EngineData>>,
    checks: &[Box<dyn StagedDataCheck>],
) -> KernelResult<()> {
    let mut summary = None;
    let mut row_count = 0usize;
    for batch in results {
        let batch = batch?;
        if batch.is_empty() {
            continue;
        }
        row_count = row_count.saturating_add(batch.len());
        if row_count == 1 {
            summary = Some(batch);
        }
    }
    if row_count != 1 {
        return Err(invalid_summary(format!(
            "Expected exactly one pre-commit validation row, but got {}",
            row_count,
        )));
    }
    let batch = summary.ok_or_else(|| invalid_summary("Missing pre-commit validation row"))?;
    CheckResultVisitor { checks }.visit_rows_of(batch.as_ref())
}

fn build_input(
    adds: &[RelationRef],
    removes: &[RelationRef],
    stats_schema: &StructType,
) -> KernelResult<PlanBuilder> {
    let schema = Arc::new(
        StructPatchBuilder::<StructField>::new()
            .replace_at(
                ["add"],
                "stats",
                StructField::nullable("stats", stats_schema.clone()),
            )
            .build(&INPUT_SCHEMA)?,
    );
    let add_type = schema.field_at(&ColumnName::new(["add"]))?.data_type();
    let remove_type = schema.field_at(&ColumnName::new(["remove"]))?.data_type();
    let mut inputs = Vec::new();
    for (relations, is_add) in [(adds, true), (removes, false)] {
        for relation in relations {
            let source_type = if is_add { add_type } else { remove_type };
            let action = build_nested_expr(relation.schema(), ColumnName::default(), source_type)?;
            let row = if is_add {
                Expression::struct_from([action, null_lit(remove_type.clone())])
            } else {
                Expression::struct_from([null_lit(add_type.clone()), action])
            };
            inputs
                .push(PlanBuilder::relation_source(relation.clone()).project(row, schema.clone())?);
        }
    }
    if inputs.is_empty() {
        PlanBuilder::values(schema, vec![])
    } else {
        PlanBuilder::union_all(inputs)
    }
}

// Starting from an empty source path, build one struct expression per shared parent in the
// output schema. Leaves read the original relation path without the `add` or `remove` prefix.
// For example, the output `add.stats.numRecords` leaf reads `stats.numRecords` from an add
// relation; the caller wraps that source expression under the output `add` struct.
// Missing source fields become typed nulls. Nested source structs preserve their parent validity;
// the outer action struct stays present even when all its fields are null.
fn build_nested_expr(
    source_schema: &StructType,
    column: ColumnName,
    data_type: &DataType,
) -> KernelResult<Expression> {
    let source_field = if column.is_empty() {
        None
    } else {
        match source_schema.field_at(&column) {
            Ok(field) => Some(field),
            Err(_) => return Ok(null_lit(data_type.clone())),
        }
    };
    if let DataType::Struct(schema) = data_type {
        if source_field.is_some_and(|field| !matches!(field.data_type(), DataType::Struct(_))) {
            return Err(KernelError::schema(format!("Expected struct at {column}")));
        }
        let fields = schema
            .fields()
            .map(|field| {
                let path = ColumnName::new(column.iter().chain(std::iter::once(field.name())));
                build_nested_expr(source_schema, path, field.data_type())
            })
            .collect::<KernelResult<Vec<_>>>()?;
        return Ok(if column.is_empty() {
            Expression::struct_from(fields)
        } else {
            Expression::struct_with_nullability_from(
                fields,
                Expression::from(Predicate::is_not_null(Expression::column(column))),
            )
        });
    }
    if source_field.is_some_and(|field| field.data_type() != data_type) {
        return Err(KernelError::schema(format!(
            "Unexpected input type at {column}; expected {data_type}"
        )));
    }
    Ok(Expression::column(column))
}

fn invalid_summary(message: impl AsRef<str>) -> KernelError {
    KernelError::internal_error(format!(
        "Invalid pre-commit validation result: {}",
        message.as_ref()
    ))
}

/// Reads the fixed summary projection in one visit, checking rules in selection order.
// Only column metadata needs a static lifetime. Getters stay borrowed inside `visit`, where error
// fields are copied into owned Scalars before constructing the error.
struct CheckResultVisitor<'a> {
    checks: &'a [Box<dyn StagedDataCheck>],
}

impl CheckResultVisitor<'_> {
    fn read<'a>(getter: &'a dyn GetData<'a>, data_type: &DataType) -> KernelResult<Scalar> {
        let value = match *data_type {
            DataType::STRING => getter
                .get_str(0, "check result")?
                .map(|value| Scalar::String(value.to_owned())),
            DataType::LONG => getter.get_long(0, "check result")?.map(Scalar::Long),
            DataType::BOOLEAN => getter.get_bool(0, "check result")?.map(Scalar::Boolean),
            _ => return Err(invalid_summary("Unsupported error-info field type")),
        };
        Ok(value.unwrap_or_else(|| Scalar::Null(data_type.clone())))
    }
}

impl RowVisitor for CheckResultVisitor<'_> {
    fn selected_column_names_and_types(&self) -> (&'static [ColumnName], &'static [DataType]) {
        RESULT_COLUMNS.as_ref()
    }

    fn visit<'a>(&mut self, row_count: usize, getters: &[&'a dyn GetData<'a>]) -> KernelResult<()> {
        if row_count != 1 {
            return Err(invalid_summary("Expected one check result row"));
        }
        if getters.len() != RESULT_COLUMNS.as_ref().0.len() {
            return Err(invalid_summary("Missing check result fields"));
        }
        for check in self.checks {
            let offset = check.kind().result_offset();
            match getters[offset].get_bool(0, "check validity")? {
                Some(true) => continue,
                Some(false) => (),
                None => return Err(invalid_summary("Missing check validity")),
            }
            let fields: Vec<_> = check.kind().error_schema().fields().cloned().collect();
            let values = fields
                .iter()
                .zip(&getters[offset + 1..])
                .map(|(field, getter)| Self::read(*getter, field.data_type()))
                .collect::<KernelResult<Vec<_>>>()?;
            let sample = StructData::try_new(fields, values)?;
            return Err(check.build_error(&sample)?);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests;
