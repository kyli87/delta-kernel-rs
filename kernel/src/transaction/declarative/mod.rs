mod precommit;
#[cfg(test)]
mod test_utils;

use precommit::{
    AddFileFieldsCheck, AppendOnlyCheck, CheckList, ClusteringStatsCheck, NumRecordsCheck,
    RemoveFileFieldsCheck,
};

use std::sync::Arc;
use crate::actions::{Add, Remove};
use crate::schema::{SchemaRef, ToSchema};

use super::{CommitResult, Declarative, Transaction, UpdateTable};
#[cfg(doc)]
use crate::actions::deletion_vector::DeletionVectorDescriptor;
use crate::plans::{Operation, RelationRef, ScopedPlanExecutor};
#[cfg(doc)]
use crate::scan::scan_row_schema;
use crate::table_features::TableFeature;
use crate::{KernelError, KernelResult};

impl<S> Transaction<S, Declarative> {
    /// Consume the transaction and commit it to the table.
    ///
    /// See [`Transaction::commit`] for result semantics and validation requirements.
    /// The executor runs plans against staged relations, which must remain available in its scope.
    pub fn commit_declarative(
        self,
        _executor: &dyn ScopedPlanExecutor,
    ) -> KernelResult<CommitResult<S, Declarative>> {
        Err(KernelError::unsupported(
            "Declarative transaction commits are not implemented",
        ))
    }

    /// Stages connector-prepared Add actions without rewriting physical metadata.
    ///
    /// The flat input schema is given by [`Self::add_actions_schema`]. Optional fields may be
    /// omitted. JSON statistics, tags, deletion vectors and assigned row IDs are preserved.
    pub fn add_actions_ref(&mut self, actions: RelationRef) {
        self.staged_data_changes.add_actions.push(actions);
    }

    /// Returns the flat schema for connector-prepared Add actions.
    pub fn add_actions_schema() -> SchemaRef {
        Arc::new(Add::to_schema())
    }

    /// Returns the flat schema for connector-prepared Remove actions.
    pub fn remove_actions_schema() -> SchemaRef {
        Arc::new(Remove::to_schema())
    }

    /// Supplies exactly one row of connector commit-info fields. Kernel-owned fields override
    /// connector values, while other fields and connector tags are retained.
    pub fn with_commit_info_ref(mut self, commit_info: RelationRef) -> Self {
        self.staged_data_changes.commit_info = Some(commit_info);
        self
    }

    /// Add files to include in this transaction. This API generally enables the engine to
    /// add/append/insert data (files) to the table. Note that this API can be called multiple times
    /// to add multiple relations.
    ///
    /// The expected schema for `add_metadata` is given by [`Transaction::add_files_schema`].
    /// The relation must remain available in the executor passed to
    /// [`commit_declarative`](Self::commit_declarative) until commit completes. Staging does not
    /// execute the relation or validate its rows.
    pub fn add_files_ref(&mut self, add_metadata: RelationRef) {
        self.staged_data_changes.add_files.push(add_metadata);
    }

    /// Performs validation against this transaction's staged file actions by executing a
    /// declarative plan (containing the needed set of validations). See `precommit` module for
    /// pla-based check logic.
    #[allow(dead_code)]
    fn validate_file_actions(&self, executor: &dyn ScopedPlanExecutor) -> KernelResult<()> {
        // TODO: DV updates is not yet supported
        if !self.staged_data_changes.dv_updates.is_empty() {
            return Err(KernelError::unsupported(
                "Declarative validation of staged DV updates",
            ));
        }

        let checks = self.file_action_checks()?;
        let plan = precommit::build_plan(
            &self.staged_data_changes.add_files,
            &self.staged_data_changes.remove_files,
            &checks,
            self.stats_schema()?.as_ref(),
        )?;
        let result = executor
            .execute_op(Operation::QueryPlan(plan))?
            .into_data()?;
        precommit::validate_result(result, &checks)
    }

    /// Selects which pre-commit check should be run (based on the effective table configuration
    /// and transaction intent.)
    fn file_action_checks(&self) -> KernelResult<CheckList> {
        let mut checks: CheckList = vec![
            Box::new(AddFileFieldsCheck),
            Box::new(RemoveFileFieldsCheck),
        ];

        if self.data_change
            && self
                .effective_table_config
                .is_feature_enabled(&TableFeature::AppendOnly)
        {
            checks.push(Box::new(AppendOnlyCheck));
        }

        if self.effective_table_config.requires_stats_num_records() {
            checks.push(Box::new(NumRecordsCheck));
        }

        let physical_schema = self.effective_table_config.physical_schema();
        let columns = self
            .physical_clustering_columns
            .iter()
            .flatten()
            .map(|column| {
                Ok((
                    column.clone(),
                    physical_schema.field_at(column)?.data_type().clone(),
                ))
            })
            .collect::<KernelResult<Vec<_>>>()?;
        if !columns.is_empty() {
            checks.push(Box::new(ClusteringStatsCheck::new(columns)?));
        }
        Ok(checks)
    }
}

impl Transaction<UpdateTable, Declarative> {
    /// Stages connector-prepared Remove actions with the flat [`Self::remove_actions_schema`].
    /// Optional fields may be omitted. The connector's deletion timestamp, tags and row IDs are
    /// preserved, and the actions undergo the same pre-commit checks as generated actions.
    pub fn remove_actions_ref(&mut self, actions: RelationRef) {
        self.staged_data_changes.remove_actions.push(actions);
    }

    /// Remove files from the table in this transaction. This API generally enables the engine to
    /// delete data (at file-level granularity) from the table. Note that this API can be called
    /// multiple times to remove multiple relations.
    ///
    /// The expected schema for `remove_metadata` is given by
    /// [`scan_row_schema`]. The relation must contain only the rows selected for removal and remain
    /// available in the executor passed to [`commit_declarative`](Self::commit_declarative) until
    /// commit completes. Staging does not execute the relation or validate its rows.
    pub fn remove_files_ref(&mut self, remove_metadata: RelationRef) {
        self.staged_data_changes.remove_files.push(remove_metadata);
    }

    /// Update deletion vectors for files in the table.
    ///
    /// This method can be called multiple times to update deletion vectors for different sets of
    /// files. It stages relations to match by file path and generate remove/add actions at commit.
    /// Staging does not execute the relations or validate their rows.
    ///
    /// # Arguments
    ///
    /// * `new_dv_descriptors` - A relation with a string `path` column and a `deletionVector`
    ///   column containing the new [`DeletionVectorDescriptor`] for each file.
    /// * `existing_data_files` - Scan file metadata using [`scan_row_schema`], covering every path
    ///   in `new_dv_descriptors`. Every row in `existing_data_files` must have an accurate
    ///   `stats.numRecords` value.
    ///
    /// Both relations must remain available in the executor passed to
    /// [`commit_declarative`](Self::commit_declarative) until commit completes.
    pub fn update_deletion_vectors_ref(
        &mut self,
        new_dv_descriptors: RelationRef,
        existing_data_files: RelationRef,
    ) {
        self.staged_data_changes
            .dv_updates
            .push((new_dv_descriptors, existing_data_files));
    }
}

#[cfg(test)]
mod tests;
