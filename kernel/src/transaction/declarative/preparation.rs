//! Declarative action preparation and commit orchestration.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

use super::actions::{normalize, reject, retain, scalar, values, wrap};
use super::{Declarative, Transaction, precommit};
use crate::action_reconciliation::plan::MetadataPlanner;
use crate::action_reconciliation::plan::action_set::ActionSet;
use crate::actions::visitors::DomainMetadataVisitor;
use crate::actions::{
    Add, CommitInfo, DomainMetadata, LOG_COMMIT_INFO_SCHEMA, LOG_DOMAIN_METADATA_SCHEMA,
    LOG_METADATA_SCHEMA, LOG_PROTOCOL_SCHEMA, LOG_TXN_SCHEMA, Remove,
};
use crate::committer::CommitResponse;
use crate::crc::FileStatsDelta;
use crate::expressions::{Expression, Predicate, col, column_name, lit, null_lit};
use crate::plans::ir::nodes::Agg;
use crate::plans::{Operation, PlanBuilder, RelationRef, ScopedPlanExecutor};
use crate::row_tracking::{
    ROW_TRACKING_DOMAIN_NAME, RowTrackingDomainMetadata, parse_row_tracking_high_water_mark,
};
use crate::schema::{DataType, SchemaRef, StructField, StructType, ToSchema, schema_ref};
use crate::table_features::TableFeature;
use crate::transaction::commit_info::{CommitInfoTagsVisitor, commit_info_literal_exprs};
use crate::transaction::{CommitResult, NonfileCommitActions};
use crate::{KernelError, KernelResult, Result, RowVisitor, version_as_i64};

impl<S> Transaction<S, Declarative> {
    pub(super) fn commit_prepared(
        self,
        executor: &dyn ScopedPlanExecutor,
    ) -> Result<CommitResult<S, Declarative>> {
        let start = Instant::now();
        self.validate_operation_compatibility()?;
        self.validate_commit()?;
        if self.is_adaptive_metadata_enabled() {
            return Err(
                KernelError::unsupported("Declarative adaptive metadata transactions").into(),
            );
        }
        if self.engine_commit_info.is_some() {
            return Err(KernelError::invalid_transaction_state(
                "Use with_commit_info_ref for declarative transactions",
            )
            .into());
        }
        let mut domains = self.load_domains(executor)?;
        let nonfile = self.nonfile_actions(executor)?;
        let (adds, removes, row_tracking) = self.prepare_file_relations(executor, &domains)?;
        self.validate_relations(executor, &adds, &removes)?;
        let mut domain_changes = self.system_domain_metadata_additions.clone();
        let calculated_hwm =
            row_tracking.or_else(|| self.provided_row_tracking_high_water_mark.map(|_| -1));
        if let Some(calculated) = calculated_hwm {
            let current = domains
                .get(ROW_TRACKING_DOMAIN_NAME)
                .map(|dm| parse_row_tracking_high_water_mark(dm.configuration()))
                .transpose()?
                .unwrap_or(-1);
            let calculated = calculated.max(current);
            let high_water_mark = self
                .provided_row_tracking_high_water_mark
                .unwrap_or(calculated);
            if high_water_mark < calculated {
                return Err(KernelError::invalid_transaction_state(
                    "Provided row-tracking high-water mark is below the calculated value",
                )
                .into());
            }
            domain_changes.push(DomainMetadata::try_from(RowTrackingDomainMetadata::new(
                high_water_mark,
            ))?);
        }
        domain_changes.extend(self.user_domain_metadata_additions.clone());
        for domain in &self.user_domain_removals {
            if let Some(previous) = domains.remove(domain) {
                domain_changes.push(DomainMetadata::remove(
                    domain.clone(),
                    previous.configuration().to_owned(),
                ));
            }
        }
        let file_stats = FileStatsDelta {
            gross_add_files: scalar(executor, source(&adds), Agg::count_star())?.unwrap_or(0)
                as u64,
            gross_remove_files: scalar(executor, source(&removes), Agg::count_star())?.unwrap_or(0)
                as u64,
            gross_add_bytes: scalar(executor, source(&adds), Agg::sum(column_name!("size")))?
                .unwrap_or(0) as u64,
            gross_remove_bytes: scalar(executor, source(&removes), Agg::sum(column_name!("size")))?
                .unwrap_or(0) as u64,
            net_histogram: None,
        };
        let commit_info = self.commit_info_plan(executor, nonfile.kernel_commit_info.clone())?;
        let commit_schema = schema_ref! {
            ..(commit_info.schema().fields()),
            nullable "protocol": (crate::actions::Protocol::to_schema()),
            nullable "metaData": (crate::actions::Metadata::to_schema()),
            nullable "add": (Add::to_schema()),
            nullable "remove": (Remove::to_schema()),
            nullable "txn": (crate::actions::SetTransaction::to_schema()),
            nullable "domainMetadata": (DomainMetadata::to_schema()),
        };
        let mut actions = vec![retain(
            executor,
            "commit_info",
            normalize(commit_info, commit_schema.clone())?,
        )?];
        if let Some(protocol) = &nonfile.protocol {
            actions.push(values(
                executor,
                "protocol",
                LOG_PROTOCOL_SCHEMA.clone(),
                vec![vec![protocol.clone().into()]],
                &commit_schema,
            )?);
        }
        if let Some(metadata) = &nonfile.metadata {
            actions.push(values(
                executor,
                "metadata",
                LOG_METADATA_SCHEMA.clone(),
                vec![vec![metadata.clone().into()]],
                &commit_schema,
            )?);
        }
        for (name, reference) in [("add", adds), ("remove", removes)] {
            actions.push(retain(
                executor,
                name,
                normalize(wrap(source(&reference), name)?, commit_schema.clone())?,
            )?);
        }
        if !nonfile.set_transactions.is_empty() {
            actions.push(values(
                executor,
                "transactions",
                LOG_TXN_SCHEMA.clone(),
                nonfile
                    .set_transactions
                    .iter()
                    .cloned()
                    .map(|txn| vec![txn.into()])
                    .collect(),
                &commit_schema,
            )?);
        }
        if !domain_changes.is_empty() {
            actions.push(values(
                executor,
                "domains",
                LOG_DOMAIN_METADATA_SCHEMA.clone(),
                domain_changes
                    .iter()
                    .cloned()
                    .map(|dm| vec![dm.into()])
                    .collect(),
                &commit_schema,
            )?);
        }
        let commit_metadata = self.create_commit_metadata(
            nonfile.commit_version,
            nonfile.kernel_commit_info.in_commit_timestamp,
            nonfile.protocol,
            nonfile.metadata,
            domain_changes.clone(),
        )?;
        let num_dv_updates =
            self.staged_data_changes
                .dv_updates
                .iter()
                .try_fold(0u64, |count, (dvs, _)| {
                    Ok::<_, KernelError>(
                        count
                            + scalar(executor, source(dvs), Agg::count_star())?.unwrap_or(0) as u64,
                    )
                })?;
        let crc_delta = self.build_crc_delta(
            file_stats.clone(),
            nonfile.kernel_commit_info,
            domain_changes,
        )?;
        let prepare_duration = start.elapsed();
        let commit_start = Instant::now();
        let response = self.committer.commit_declarative(
            executor,
            Box::new(actions.into_iter()),
            commit_metadata,
        );
        let committer_duration = commit_start.elapsed();
        match response {
            Ok(CommitResponse::Committed { file_meta }) => {
                self.record_commit_success_metrics(
                    &file_stats,
                    num_dv_updates,
                    prepare_duration,
                    committer_duration,
                );
                Ok(CommitResult::Committed(self.into_committed(
                    file_meta,
                    crc_delta,
                    #[cfg(feature = "adaptive-metadata-in-dev")]
                    None,
                )?))
            }
            Ok(CommitResponse::Conflict { version }) => {
                tracing::Span::current().record(
                    "failure_reason",
                    crate::metrics::CommitFailureReason::Conflict.as_ref(),
                );
                Ok(CommitResult::Conflicted(self.into_conflicted(version)))
            }
            Err(error) if matches!(error.without_backtrace(), KernelError::IOError(_)) => {
                Ok(CommitResult::Retryable(self.into_retryable(error)))
            }
            Err(error) => Err(error.into()),
        }
    }

    fn nonfile_actions(
        &self,
        executor: &dyn ScopedPlanExecutor,
    ) -> KernelResult<NonfileCommitActions> {
        let ict = if !self
            .effective_table_config
            .is_feature_enabled(&TableFeature::InCommitTimestamp)
        {
            None
        } else if self.is_create_table() {
            Some(self.commit_timestamp)
        } else {
            let snapshot = self.read_snapshot()?;
            let file = snapshot
                .log_segment()
                .listed
                .latest_commit_file
                .as_ref()
                .ok_or(KernelError::MissingVersion(snapshot.version()))?;
            let plan = PlanBuilder::scan_json(
                [file.location.clone()],
                &[],
                LOG_COMMIT_INFO_SCHEMA.clone(),
            )?;
            let previous = scalar(
                executor,
                plan,
                Agg::max(column_name!("commitInfo.inCommitTimestamp")),
            )?
            .ok_or_else(|| {
                KernelError::invalid_transaction_state("Previous commit has no in-commit timestamp")
            })?;
            Some(
                self.commit_timestamp
                    .max(previous.checked_add(1).ok_or_else(|| {
                        KernelError::invalid_transaction_state("In-commit timestamp overflow")
                    })?),
            )
        };
        let mut info = CommitInfo::new(
            self.commit_timestamp,
            ict,
            Some(self.operation.to_string()),
            Some(self.engine_info.clone()),
            self.is_blind_append,
        );
        if let Some(parameters) = self.operation_parameters.clone() {
            info.set_operation_parameters(parameters);
        }
        if let Some(metrics) = self.operation_metrics.clone() {
            info.set_operation_metrics(metrics);
        }
        if !self.is_create_table()
            && self
                .effective_table_config
                .is_feature_enabled(&TableFeature::RowTracking)
        {
            info.set_row_tracking_preserved();
        }
        Ok(NonfileCommitActions {
            commit_version: self.get_commit_version(),
            kernel_commit_info: info,
            protocol: self
                .should_emit_protocol
                .then(|| self.effective_table_config.protocol().clone()),
            metadata: self
                .should_emit_metadata
                .then(|| self.effective_table_config.metadata().clone()),
            set_transactions: self.set_transactions.clone(),
        })
    }

    fn load_domains(
        &self,
        executor: &dyn ScopedPlanExecutor,
    ) -> KernelResult<HashMap<String, DomainMetadata>> {
        if self.is_create_table()
            || (self.user_domain_removals.is_empty()
                && self.provided_row_tracking_high_water_mark.is_none()
                && !self
                    .effective_table_config
                    .should_assign_fresh_row_tracking_metadata())
        {
            return Ok(HashMap::new());
        }
        let plan = MetadataPlanner::new(
            self.read_snapshot()?.log_segment(),
            ActionSet {
                domain_metadata: true,
                ..Default::default()
            },
        )
        .replay()?
        .build()?;
        let mut visitor = DomainMetadataVisitor::new(None);
        for batch in executor
            .execute_op(Operation::QueryPlan(plan))?
            .into_data()?
        {
            visitor.visit_rows_of(batch?.as_ref())?;
        }
        Ok(visitor.into_domain_metadatas())
    }

    fn commit_info_plan(
        &self,
        executor: &dyn ScopedPlanExecutor,
        mut info: CommitInfo,
    ) -> KernelResult<PlanBuilder> {
        let Some(reference) = self.staged_data_changes.commit_info.as_ref() else {
            return PlanBuilder::values(LOG_COMMIT_INFO_SCHEMA.clone(), vec![vec![info.into()]]);
        };
        if scalar(executor, source(reference), Agg::count_star())? != Some(1) {
            return Err(KernelError::invalid_transaction_state(
                "Connector commit info must contain exactly one row",
            ));
        }
        if reference.schema().contains("tags") {
            let mut visitor = CommitInfoTagsVisitor::default();
            let tags = source(reference)
                .project(
                    Expression::struct_from([col!("tags")]),
                    reference.schema().project(&["tags"])?,
                )?
                .build()?;
            for batch in executor
                .execute_op(Operation::QueryPlan(tags))?
                .into_data()?
            {
                visitor.visit_rows_of(batch?.as_ref())?;
            }
            info.merge_tags(visitor.tags);
        }
        let literals = commit_info_literal_exprs(info)?;
        let kernel_schema = CommitInfo::to_schema();
        let mut expressions = Vec::new();
        let mut fields = Vec::new();
        for field in reference.schema().fields() {
            if let Some((_, value)) = literals.iter().find(|(name, _)| *name == field.name()) {
                fields.push(
                    kernel_schema
                        .field(field.name())
                        .ok_or_else(|| KernelError::internal_error("Missing commit info field"))?
                        .clone(),
                );
                expressions.push(value.clone());
            } else {
                fields.push(field.clone());
                expressions.push(Arc::new(Expression::column([field.name().clone()])));
            }
        }
        for (name, expression) in literals {
            if !reference.schema().contains(name) {
                fields.push(
                    kernel_schema
                        .field(name)
                        .ok_or_else(|| KernelError::internal_error("Missing commit info field"))?
                        .clone(),
                );
                expressions.push(expression);
            }
        }
        wrap(
            source(reference).project(
                Expression::struct_from(expressions),
                Arc::new(StructType::try_new(fields)?),
            )?,
            "commitInfo",
        )
    }

    fn prepare_file_relations(
        &self,
        executor: &dyn ScopedPlanExecutor,
        domains: &HashMap<String, DomainMetadata>,
    ) -> KernelResult<(RelationRef, RelationRef, Option<i64>)> {
        let mut add_plans = Vec::new();
        let commit_version = version_as_i64(self.get_commit_version())?;
        let mut watermark = domains
            .get(ROW_TRACKING_DOMAIN_NAME)
            .map(|dm| parse_row_tracking_high_water_mark(dm.configuration()))
            .transpose()?
            .unwrap_or(-1);
        let assign_ids = self
            .effective_table_config
            .should_assign_fresh_row_tracking_metadata();
        for reference in &self.staged_data_changes.add_files {
            let mut plan = source(reference);
            if assign_ids {
                reject(
                    executor,
                    plan.clone(),
                    Predicate::or(
                        col!("stats.numRecords").is_null(),
                        col!("stats.numRecords").lt(lit(0i64)),
                    ),
                    "Row tracking requires non-negative stats.numRecords",
                )?;
                let count = scalar(
                    executor,
                    plan.clone(),
                    Agg::sum(column_name!("stats.numRecords")),
                )?
                .unwrap_or(0);
                let start = watermark
                    .checked_add(1)
                    .ok_or_else(|| KernelError::invalid_transaction_state("Row ID overflow"))?;
                watermark = watermark
                    .checked_add(count)
                    .ok_or_else(|| KernelError::invalid_transaction_state("Row ID overflow"))?;
                plan = plan
                    .prefix_sum(column_name!("stats.numRecords"), "_row_offset")?
                    .project_patch(|p| {
                        p.append(
                            StructField::nullable("baseRowId", DataType::LONG),
                            col!("_row_offset") + lit(start),
                        )
                        .append(
                            StructField::nullable("defaultRowCommitVersion", DataType::LONG),
                            lit(commit_version),
                        )
                        .drop("_row_offset")
                    })?;
            }
            plan = plan.project_patch(|p| {
                p.append(
                    StructField::nullable("dataChange", DataType::BOOLEAN),
                    lit(self.data_change),
                )
                .replace(
                    "stats",
                    StructField::nullable("stats", DataType::STRING),
                    Expression::unary(crate::expressions::UnaryExpressionOp::ToJson, col!("stats")),
                )
            })?;
            add_plans.push(normalize(plan, Self::add_actions_schema())?);
        }
        for reference in &self.staged_data_changes.add_actions {
            add_plans.push(normalize(source(reference), Self::add_actions_schema())?);
        }
        let mut remove_plans = Vec::new();
        for reference in &self.staged_data_changes.remove_files {
            remove_plans.push(self.scan_action(source(reference), false)?);
        }
        for (new_dvs, files) in &self.staged_data_changes.dv_updates {
            let (adds, removes) = self.prepare_dv_updates(executor, new_dvs, files)?;
            add_plans.push(adds);
            remove_plans.push(removes);
        }
        for reference in &self.staged_data_changes.remove_actions {
            remove_plans.push(normalize(source(reference), Self::remove_actions_schema())?);
        }
        let adds = retain(
            executor,
            "prepared_adds",
            union_or_empty(add_plans, Self::add_actions_schema())?,
        )?;
        let removes = retain(
            executor,
            "prepared_removes",
            union_or_empty(remove_plans, Self::remove_actions_schema())?,
        )?;
        if assign_ids {
            let parsed = source(&adds).project_patch(|p| {
                p.append(
                    StructField::nullable("parsed", schema_ref! { nullable "numRecords": LONG }),
                    Expression::parse_json_strict(
                        col!("stats"),
                        schema_ref! { nullable "numRecords": LONG },
                    ),
                )
            })?;
            reject(
                executor,
                parsed.clone(),
                Predicate::or_from([
                    col!("baseRowId").is_null(),
                    col!("baseRowId").lt(lit(0i64)),
                    col!("defaultRowCommitVersion").is_null(),
                    col!("defaultRowCommitVersion").lt(lit(0i64)),
                    col!("parsed.numRecords").is_null(),
                    col!("parsed.numRecords").lt(lit(0i64)),
                ]),
                "Row tracking requires valid row IDs and stats.numRecords",
            )?;
            let maximum = parsed.project(
                Expression::struct_from(
                    [col!("baseRowId") + col!("parsed.numRecords") - lit(1i64)],
                ),
                schema_ref! { nullable "last": LONG },
            )?;
            watermark = watermark
                .max(scalar(executor, maximum, Agg::max(column_name!("last")))?.unwrap_or(-1));
        }
        let publish_watermark = assign_ids
            && (self.is_create_table()
                || !self.staged_data_changes.add_files.is_empty()
                || !self.staged_data_changes.add_actions.is_empty());
        Ok((adds, removes, publish_watermark.then_some(watermark)))
    }

    fn scan_action(&self, input: PlanBuilder, add: bool) -> KernelResult<PlanBuilder> {
        let schema = if add {
            Self::add_actions_schema()
        } else {
            Self::remove_actions_schema()
        };
        let stats = if input.schema().contains("stats_parsed") {
            Expression::coalesce([
                col!("stats"),
                Expression::unary(
                    crate::expressions::UnaryExpressionOp::ToJson,
                    col!("stats_parsed"),
                ),
            ])
        } else {
            col!("stats")
        };
        let expressions = schema
            .fields()
            .map(|field| match field.name().as_str() {
                "dataChange" => lit(self.data_change),
                "deletionTimestamp" => lit(self.commit_timestamp),
                "extendedFileMetadata" => Expression::from_pred(Predicate::and(
                    col!("size").is_not_null(),
                    col!("fileConstantValues.partitionValues").is_not_null(),
                )),
                "stats" => stats.clone(),
                "partitionValues"
                | "tags"
                | "baseRowId"
                | "defaultRowCommitVersion"
                | "clusteringProvider" => Expression::column(["fileConstantValues", field.name()]),
                name if input.schema().contains(name) => Expression::column([name]),
                _ => null_lit(field.data_type().clone()),
            })
            .collect::<Vec<_>>();
        input.project(Expression::struct_from(expressions), schema)
    }

    fn prepare_dv_updates(
        &self,
        executor: &dyn ScopedPlanExecutor,
        new_dvs: &RelationRef,
        files: &RelationRef,
    ) -> KernelResult<(PlanBuilder, PlanBuilder)> {
        if !self
            .effective_table_config
            .is_feature_enabled(&TableFeature::DeletionVectors)
        {
            return Err(KernelError::unsupported(
                "Deletion vector writes require deletionVectors enabled",
            ));
        }
        let dv_schema = crate::actions::deletion_vector::DeletionVectorDescriptor::to_schema();
        let old_schema = crate::scan::data_skipping::stats_schema::schema_with_all_fields_nullable(
            &Self::add_actions_schema(),
        );
        let joined_schema = schema_ref! {
            nullable "path": STRING,
            nullable "old": (old_schema.clone()),
            nullable "new": (dv_schema.clone()),
        };
        let restored = normalize(
            self.scan_action(source(files), true)?,
            old_schema.clone().into(),
        )?;
        let restored_struct = Expression::struct_from(
            restored
                .schema()
                .fields()
                .map(|f| Expression::column([f.name().clone()])),
        );
        let left = restored.project(
            Expression::struct_from([col!("path"), restored_struct, null_lit(dv_schema.clone())]),
            joined_schema.clone(),
        )?;
        let right = source(new_dvs).project(
            Expression::struct_from([col!("path"), null_lit(old_schema), col!("deletionVector")]),
            joined_schema,
        )?;
        reject(
            executor,
            right.clone(),
            Predicate::or(col!("path").is_null(), col!("new").is_null()),
            "DV updates require a path and deletion vector",
        )?;
        let joined =
            PlanBuilder::union_all([left, right])?.aggregate_by([column_name!("path")], |a| {
                a.first_non_null(column_name!("old"))
                    .first_non_null(column_name!("new"))
                    .aggregate_as(Agg::count(column_name!("old")), "old_count")
                    .aggregate_as(Agg::count(column_name!("new")), "new_count")
            })?;
        reject(
            executor,
            joined.clone(),
            Predicate::and(
                col!("new_count").gt(lit(0i64)),
                Predicate::or(
                    col!("new_count").ne(lit(1i64)),
                    col!("old_count").ne(lit(1i64)),
                ),
            ),
            "Each DV update must match exactly one existing file",
        )?;
        let joined = joined.filter(col!("new_count").eq(lit(1i64)))?;
        let joined = retain(executor, "matched_dvs", joined)?;
        let stats_schema = schema_ref! { ..(self.stats_schema()?.fields().filter(|f| f.name() != "tightBounds")), nullable "tightBounds": BOOLEAN };
        let parsed = source(&joined).project_patch(|p| {
            p.append(
                StructField::nullable("parsed", stats_schema.clone()),
                Expression::parse_json_strict(col!("old.stats"), stats_schema.clone()),
            )
        })?;
        reject(
            executor,
            parsed.clone(),
            Predicate::or(
                col!("parsed.numRecords").is_null(),
                col!("parsed.numRecords").lt(lit(0i64)),
            ),
            "DV updates require non-negative stats.numRecords",
        )?;
        let parsed = parsed.project_patch(|p| {
            p.replace_at(
                ["parsed"],
                "tightBounds",
                StructField::nullable("tightBounds", DataType::BOOLEAN),
                lit(false),
            )
        })?;
        let adds = parsed.project(
            Expression::struct_from(Self::add_actions_schema().fields().map(|field| {
                match field.name().as_str() {
                    "deletionVector" => col!("new"),
                    "stats" => Expression::unary(
                        crate::expressions::UnaryExpressionOp::ToJson,
                        col!("parsed"),
                    ),
                    name => Expression::column(["old", name]),
                }
            })),
            Self::add_actions_schema(),
        )?;
        let removes = source(&joined).project(
            Expression::struct_from(Self::remove_actions_schema().fields().map(|field| {
                match field.name().as_str() {
                    "deletionTimestamp" => lit(self.commit_timestamp),
                    "extendedFileMetadata" => lit(true),
                    name if Self::add_actions_schema().contains(name) => {
                        Expression::column(["old", name])
                    }
                    _ => null_lit(field.data_type().clone()),
                }
            })),
            Self::remove_actions_schema(),
        )?;
        Ok((adds, removes))
    }

    fn validate_relations(
        &self,
        executor: &dyn ScopedPlanExecutor,
        adds: &RelationRef,
        removes: &RelationRef,
    ) -> KernelResult<()> {
        let stats_schema = self.stats_schema()?;
        let check_adds = source(adds).project_patch(|p| {
            p.replace(
                "stats",
                StructField::nullable("stats", stats_schema.clone()),
                Expression::parse_json_strict(col!("stats"), stats_schema.clone()),
            )
        })?;
        let check_adds = retain(executor, "validated_adds", check_adds)?;
        let checks = self.file_action_checks()?;
        let plan = precommit::build_plan(
            &[check_adds],
            std::slice::from_ref(removes),
            &checks,
            &stats_schema,
        )?;
        precommit::validate_result(
            executor
                .execute_op(Operation::QueryPlan(plan))?
                .into_data()?,
            &checks,
        )?;
        let partition_columns = self
            .effective_table_config
            .physical_partition_columns()
            .collect::<Vec<_>>();
        let partition_keys = source(adds).project_patch(|p| {
            p.append(
                StructField::nullable(
                    "_partition_keys",
                    crate::schema::ArrayType::new(DataType::STRING, false),
                ),
                Expression::unary(
                    crate::expressions::UnaryExpressionOp::MapKeys,
                    col!("partitionValues"),
                ),
            )
            .append(
                StructField::nullable("_partition_count", DataType::LONG),
                Expression::unary(
                    crate::expressions::UnaryExpressionOp::Cardinality,
                    col!("partitionValues"),
                ),
            )
        })?;
        let invalid_partitions = Predicate::or_from([
            col!("partitionValues").is_null(),
            col!("_partition_count").ne(lit(partition_columns.len() as i64)),
            Predicate::or_from(partition_columns.iter().map(|name| {
                Predicate::not(Predicate::binary(
                    crate::expressions::BinaryPredicateOp::In,
                    lit(name.clone()),
                    col!("_partition_keys"),
                ))
            })),
        ]);
        reject(
            executor,
            partition_keys,
            invalid_partitions,
            "AddFile partitionValues keys must match the physical partition columns",
        )?;
        if !self
            .effective_table_config
            .is_feature_supported(&TableFeature::ClusteredTable)
        {
            reject(
                executor,
                source(adds),
                col!("clusteringProvider").is_not_null(),
                "Clustering provider requires clustered table support",
            )?;
        }
        if self
            .effective_table_config
            .is_feature_enabled(&TableFeature::RowTracking)
        {
            reject(
                executor,
                source(removes),
                Predicate::or_from([
                    col!("baseRowId").is_null(),
                    col!("baseRowId").lt(lit(0i64)),
                    col!("defaultRowCommitVersion").is_null(),
                    col!("defaultRowCommitVersion").lt(lit(0i64)),
                ]),
                "Removed files must preserve row tracking metadata",
            )?;
        }
        for (reference, allows_dv) in [
            (
                adds,
                self.effective_table_config
                    .is_feature_enabled(&TableFeature::DeletionVectors),
            ),
            (
                removes,
                self.effective_table_config
                    .is_feature_supported(&TableFeature::DeletionVectors),
            ),
        ] {
            if !allows_dv {
                reject(
                    executor,
                    source(reference),
                    col!("deletionVector").is_not_null(),
                    "Deletion vectors are not enabled for this action",
                )?;
            }
            if !self
                .effective_table_config
                .is_feature_supported(&TableFeature::RowTracking)
            {
                reject(
                    executor,
                    source(reference),
                    Predicate::or(
                        col!("baseRowId").is_not_null(),
                        col!("defaultRowCommitVersion").is_not_null(),
                    ),
                    "Row IDs require rowTracking support",
                )?;
            }
        }
        if !self.skip_dedup_validation {
            let keys = schema_ref! {
                nullable "path": STRING,
                nullable "storageType": STRING,
                nullable "pathOrInlineDv": STRING,
                nullable "offset": INTEGER,
            };
            let lanes = [adds, removes]
                .into_iter()
                .map(|reference| {
                    source(reference).project(
                        Expression::struct_from([
                            col!("path"),
                            col!("deletionVector.storageType"),
                            col!("deletionVector.pathOrInlineDv"),
                            col!("deletionVector.offset"),
                        ]),
                        keys.clone(),
                    )
                })
                .collect::<KernelResult<Vec<_>>>()?;
            let duplicates = PlanBuilder::union_all(lanes)?.aggregate_by(
                [
                    column_name!("path"),
                    column_name!("storageType"),
                    column_name!("pathOrInlineDv"),
                    column_name!("offset"),
                ],
                |a| a.count_star(),
            )?;
            reject(
                executor,
                duplicates,
                col!("count").gt(lit(1i64)),
                "Duplicate add/remove file identity",
            )?;
        }
        for reference in [adds, removes] {
            let has_dv = col!("deletionVector").is_not_null();
            let storage = col!("deletionVector.storageType");
            let invalid = Predicate::or_from([
                storage.clone().is_null(),
                Predicate::and_from([
                    storage.clone().ne(lit("i")),
                    storage.clone().ne(lit("u")),
                    storage.clone().ne(lit("p")),
                ]),
                col!("deletionVector.pathOrInlineDv").is_null(),
                col!("deletionVector.sizeInBytes").is_null(),
                col!("deletionVector.sizeInBytes").lt(lit(0i32)),
                col!("deletionVector.cardinality").is_null(),
                col!("deletionVector.cardinality").lt(lit(0i64)),
                col!("deletionVector.offset").lt(lit(0i32)),
                Predicate::and(
                    storage.eq(lit("i")),
                    col!("deletionVector.offset").is_not_null(),
                ),
            ]);
            reject(
                executor,
                source(reference),
                Predicate::and(has_dv, invalid),
                "Invalid deletion vector descriptor",
            )?;
        }
        for reference in [adds, removes] {
            reject(
                executor,
                source(reference),
                Predicate::or(
                    col!("dataChange").is_null(),
                    col!("dataChange").ne(lit(self.data_change)),
                ),
                "Prepared action dataChange does not match transaction",
            )?;
            if !self.skip_dedup_validation {
                let counts =
                    source(reference).aggregate_by([column_name!("path")], |a| a.count_star())?;
                reject(
                    executor,
                    counts,
                    col!("count").gt(lit(1i64)),
                    "Duplicate file action path",
                )?;
            }
        }
        Ok(())
    }
}

fn source(reference: &RelationRef) -> PlanBuilder {
    PlanBuilder::relation_source(reference.clone())
}
fn union_or_empty(plans: Vec<PlanBuilder>, schema: SchemaRef) -> KernelResult<PlanBuilder> {
    if plans.is_empty() {
        PlanBuilder::values(schema, vec![])
    } else {
        PlanBuilder::union_all(plans)
    }
}
