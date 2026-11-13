//! Plan projections and bounded scalar results used by transaction preparation.

use std::sync::{Arc, LazyLock};

use crate::engine_data::TypedGetData as _;
use crate::expressions::{ColumnName, Expression, Predicate, Scalar, column_name, null_lit};
use crate::plans::ir::nodes::Agg;
use crate::plans::{Operation, PlanBuilder, RelationRef, ScopedPlanExecutor};
use crate::schema::{DataType, SchemaRef, StructType};
use crate::{GetData, KernelError, KernelResult, RowVisitor};

/// Aligns a relation by name, filling absent nullable fields without changing present structs.
pub(super) fn normalize(input: PlanBuilder, schema: SchemaRef) -> KernelResult<PlanBuilder> {
    let expression = project_fields(input.schema(), &schema, &[])?;
    input.project(expression, schema)
}

fn project_fields(
    input: &StructType,
    output: &StructType,
    prefix: &[String],
) -> KernelResult<Expression> {
    let expressions = output
        .fields()
        .map(|field| {
            let mut path = prefix.to_vec();
            path.push(field.name().clone());
            let Some(source) = input.field(field.name()) else {
                return Ok(null_lit(field.data_type().clone()));
            };
            match (source.data_type(), field.data_type()) {
                (DataType::Struct(source), DataType::Struct(target)) => {
                    let nested = project_fields(source, target, &path)?;
                    Ok(Expression::struct_with_nullability_from(
                        match nested {
                            Expression::Struct(fields, _) => fields,
                            _ => {
                                return Err(KernelError::internal_error(
                                    "Expected a struct projection",
                                ));
                            }
                        },
                        Expression::from_pred(Expression::column(path).is_not_null()),
                    ))
                }
                (source, target) if source == target => Ok(Expression::column(path)),
                _ => Err(KernelError::schema(format!(
                    "Invalid type for transaction field {}",
                    path.join(".")
                ))),
            }
        })
        .collect::<KernelResult<Vec<_>>>()?;
    Ok(Expression::struct_from(expressions))
}

pub(super) fn wrap(input: PlanBuilder, name: &str) -> KernelResult<PlanBuilder> {
    let schema = Arc::new(StructType::try_new([
        crate::schema::StructField::nullable(name, input.schema().clone()),
    ])?);
    let fields = input
        .schema()
        .fields()
        .map(|field| Expression::column([field.name().clone()]));
    let expression = Expression::struct_from([Expression::struct_from(fields)]);
    input.project(expression, schema)
}

pub(super) fn retain(
    executor: &dyn ScopedPlanExecutor,
    name: &str,
    input: PlanBuilder,
) -> KernelResult<RelationRef> {
    executor.execute_and_retain(name, input.build()?)
}

pub(super) fn values(
    executor: &dyn ScopedPlanExecutor,
    name: &str,
    schema: SchemaRef,
    rows: Vec<Vec<Scalar>>,
    commit_schema: &SchemaRef,
) -> KernelResult<RelationRef> {
    retain(
        executor,
        name,
        normalize(PlanBuilder::values(schema, rows)?, commit_schema.clone())?,
    )
}

pub(super) fn scalar(
    executor: &dyn ScopedPlanExecutor,
    input: PlanBuilder,
    aggregate: Agg,
) -> KernelResult<Option<i64>> {
    let plan = input
        .aggregate_ungrouped(|a| a.aggregate_as(aggregate, "value"))?
        .build()?;
    let mut visitor = LongVisitor::default();
    for batch in executor
        .execute_op(Operation::QueryPlan(plan))?
        .into_data()?
    {
        visitor.visit_rows_of(batch?.as_ref())?;
    }
    if visitor.rows != 1 {
        return Err(KernelError::internal_error(
            "Transaction aggregate must return exactly one row",
        ));
    }
    Ok(visitor.value)
}

pub(super) fn reject(
    executor: &dyn ScopedPlanExecutor,
    input: PlanBuilder,
    predicate: Predicate,
    message: &str,
) -> KernelResult<()> {
    if scalar(executor, input.filter(predicate)?, Agg::count_star())?.unwrap_or(0) != 0 {
        return Err(KernelError::invalid_transaction_state(message));
    }
    Ok(())
}

#[derive(Default)]
struct LongVisitor {
    rows: usize,
    value: Option<i64>,
}

impl RowVisitor for LongVisitor {
    fn selected_column_names_and_types(&self) -> (&'static [ColumnName], &'static [DataType]) {
        static NAMES: LazyLock<[ColumnName; 1]> = LazyLock::new(|| [column_name!("value")]);
        (&*NAMES, &[DataType::LONG])
    }
    fn visit<'a>(&mut self, rows: usize, getters: &[&'a dyn GetData<'a>]) -> KernelResult<()> {
        self.rows += rows;
        for row in 0..rows {
            self.value = getters[0].get_opt(row, "value")?;
        }
        Ok(())
    }
}
