use std::collections::HashSet;

use super::{EngineDataResultIterator, Transaction};
use crate::actions::{get_log_domain_metadata_schema, DomainMetadata, INTERNAL_DOMAIN_PREFIX};
use crate::error::Error;
use crate::index::{index_domain_name, serialize_index_spec, INDEX_DOMAIN_PREFIX};
use crate::row_tracking::{RowTrackingDomainMetadata, ROW_TRACKING_DOMAIN_NAME};
use crate::table_features::TableFeature;
use crate::{DeltaResult, Engine, IntoEngineData, Version};

impl<S> Transaction<S> {
    /// Validate domain metadata operations for both create-table and existing-table transactions.
    ///
    /// Enforces the following rules:
    /// - DomainMetadata feature must be supported if any domain operations are present
    /// - System domains (in system_domain_metadata_additions) must correspond to a known feature
    /// - User domains cannot use the delta.* prefix (system-reserved)
    /// - Domain removals are not allowed in create-table transactions
    /// - No duplicate domains within a single transaction (across both user and system)
    pub(super) fn validate_domain_metadata_operations(&self) -> DeltaResult<()> {
        // Feature validation (applies to all transactions with domain operations)
        let has_domain_ops = !self.system_domain_metadata_additions.is_empty()
            || !self.user_domain_metadata_additions.is_empty()
            || !self.index_spec_additions.is_empty()
            || !self.user_domain_removals.is_empty();

        // Early return if no domain operations to validate
        if !has_domain_ops {
            return Ok(());
        }

        if !self
            .effective_table_config
            .is_feature_supported(&TableFeature::DomainMetadata)
        {
            return Err(Error::unsupported(
                "Domain metadata operations require writer version 7 and the 'domainMetadata' writer feature",
            ));
        }

        let is_create = self.is_create_table();
        let mut seen_domains = HashSet::with_capacity(
            self.system_domain_metadata_additions.len()
                + self.user_domain_metadata_additions.len()
                + self.index_spec_additions.len()
                + self.user_domain_removals.len(),
        );

        // Validate SYSTEM domain additions (from transforms, e.g., clustering)
        // System domains are only populated during create-table
        for dm in &self.system_domain_metadata_additions {
            let domain = dm.domain();

            // Validate the system domain corresponds to a known feature
            self.validate_system_domain_feature(domain)?;

            // Check for duplicates
            if !seen_domains.insert(domain) {
                return Err(Error::generic(format!(
                    "Metadata for domain {domain} already specified in this transaction"
                )));
            }
        }

        // Validate INDEX domain additions (via with_index_spec API). Each maps to a
        // `delta.index.<name>` system domain gated by the DataIndexes feature.
        let index_domains: Vec<String> = self
            .index_spec_additions
            .iter()
            .map(|(name, _)| index_domain_name(name))
            .collect();
        for domain in &index_domains {
            self.validate_system_domain_feature(domain)?;

            if !seen_domains.insert(domain.as_str()) {
                return Err(Error::generic(format!(
                    "Metadata for domain {domain} already specified in this transaction"
                )));
            }
        }

        // Validate USER domain additions (via with_domain_metadata API)
        for dm in &self.user_domain_metadata_additions {
            let domain = dm.domain();

            // Users cannot add system domains via the public API
            if domain.starts_with(INTERNAL_DOMAIN_PREFIX) {
                return Err(Error::generic(
                    "Cannot modify domains that start with 'delta.' as those are system controlled",
                ));
            }

            // Check for duplicates (spans both system and user domains)
            if !seen_domains.insert(domain) {
                return Err(Error::generic(format!(
                    "Metadata for domain {domain} already specified in this transaction"
                )));
            }
        }

        // No removals allowed for create-table.
        // Note: CreateTableTransaction does not expose with_domain_metadata_removed(),
        // so this is a defensive check. See #1768.
        if is_create && !self.user_domain_removals.is_empty() {
            return Err(Error::unsupported(
                "Domain metadata removals are not supported in create-table transactions",
            ));
        }

        // Validate domain removals (for non-create-table)
        for domain in &self.user_domain_removals {
            // Cannot remove system domains
            if domain.starts_with(INTERNAL_DOMAIN_PREFIX) {
                return Err(Error::generic(
                    "Cannot modify domains that start with 'delta.' as those are system controlled",
                ));
            }

            // Check for duplicates
            if !seen_domains.insert(domain.as_str()) {
                return Err(Error::generic(format!(
                    "Metadata for domain {domain} already specified in this transaction"
                )));
            }
        }

        Ok(())
    }

    /// Validate that a system domain corresponds to a known feature and that the feature is
    /// supported.
    ///
    /// This prevents arbitrary `delta.*` domains from being added during table creation.
    /// Each known system domain must have its corresponding feature enabled in the protocol.
    fn validate_system_domain_feature(&self, domain: &str) -> DeltaResult<()> {
        let table_config = &self.effective_table_config;

        // Map domain to its required feature
        let required_feature = match domain {
            ROW_TRACKING_DOMAIN_NAME => Some(TableFeature::RowTracking),
            // Will be changed to a constant in a follow up clustering create table feature PR
            "delta.clustering" => Some(TableFeature::ClusteredTable),
            // Per-index state domains (`delta.index.<name>`) are gated by DataIndexes.
            _ if domain.starts_with(INDEX_DOMAIN_PREFIX) => Some(TableFeature::DataIndexes),
            _ => {
                return Err(Error::generic(format!(
                    "Unknown system domain '{domain}'. Only known system domains are allowed."
                )));
            }
        };

        // If the domain requires a feature, validate it's supported
        if let Some(feature) = required_feature {
            if !table_config.is_feature_supported(&feature) {
                return Err(Error::generic(format!(
                    "System domain '{domain}' requires the '{feature}' feature to be enabled"
                )));
            }
        }

        Ok(())
    }

    /// Generate removal actions for user domain metadata by scanning the log.
    ///
    /// This performs an expensive log replay operation to fetch the previous configuration
    /// value for each domain being removed, as required by the Delta spec for tombstones.
    /// Returns an empty vector if there are no domain removals.
    pub(super) fn generate_user_domain_removal_actions(
        &self,
        engine: &dyn Engine,
    ) -> DeltaResult<Vec<DomainMetadata>> {
        if self.user_domain_removals.is_empty() {
            return Ok(vec![]);
        }

        // Scan log to fetch existing configurations for tombstones.
        // Pass the specific set of domains to remove so that log replay can terminate early
        // once all target domains have been found, instead of replaying the entire log.
        let domains: HashSet<&str> = self
            .user_domain_removals
            .iter()
            .map(String::as_str)
            .collect();
        let existing_domains = self
            .read_snapshot()?
            .get_domain_metadatas_internal(engine, Some(&domains))?;

        // Create removal tombstones with pre-image configurations
        Ok(self
            .user_domain_removals
            .iter()
            .filter_map(|domain| {
                // If domain doesn't exist in the log, this is a no-op (filter it out)
                existing_domains.get(domain).map(|existing| {
                    DomainMetadata::remove(domain.clone(), existing.configuration().to_owned())
                })
            })
            .collect())
    }

    /// Generate domain metadata actions with validation. Handle both user and system domains.
    ///
    /// Returns a tuple of `(action_iter, domain_metadata_vec)`.
    /// - The action iterator contains EngineData to be written to the commit file (`00N.json`).
    /// - The `Vec<DomainMetadata>` is used to construct a [`CrcDelta`](crate::crc::CrcDelta), which
    ///   feeds the post-commit snapshot with the domain metadata written in this transaction and
    ///   powers CRC file writes.
    ///
    /// This function may perform an expensive log replay operation if there are any domain
    /// removals. The log replay is required to fetch the previous configuration value for the
    /// domain to preserve in removal tombstones as mandated by the Delta spec.
    pub(super) fn generate_domain_metadata_actions<'a>(
        &'a self,
        engine: &'a dyn Engine,
        row_tracking_high_watermark: Option<RowTrackingDomainMetadata>,
        commit_version: Version,
    ) -> DeltaResult<(EngineDataResultIterator<'a>, Vec<DomainMetadata>)> {
        let is_create = self.is_create_table();

        // Validate domain operations (includes feature validation)
        self.validate_domain_metadata_operations()?;

        if is_create {
            // user_domain_removals already validated above, but be explicit
            debug_assert!(self.user_domain_removals.is_empty());
        }

        // Generate removal actions (empty for create-table due to validation above)
        let removal_actions = self.generate_user_domain_removal_actions(engine)?;

        // Generate row tracking domain action.
        let row_tracking_domain_action = row_tracking_high_watermark
            .map(DomainMetadata::try_from)
            .transpose()?
            .into_iter();

        // Generate index state domain actions. `covers_version` is stamped with the resolved
        // commit version (overwriting any caller-provided value) so it stays consistent with the
        // data committed in this transaction.
        let index_domain_actions = self
            .index_spec_additions
            .iter()
            .map(|(name, spec)| {
                let mut spec = spec.clone();
                spec.covers_version = commit_version;
                Ok(DomainMetadata::new(
                    index_domain_name(name),
                    serialize_index_spec(&spec)?,
                ))
            })
            .collect::<DeltaResult<Vec<_>>>()?;

        // Chain all domain actions: system domains, row tracking, index domains, user domains,
        // removals
        let dm_actions_vec: Vec<DomainMetadata> = self
            .system_domain_metadata_additions
            .iter()
            .cloned()
            .chain(row_tracking_domain_action)
            .chain(index_domain_actions)
            .chain(self.user_domain_metadata_additions.iter().cloned())
            .chain(removal_actions)
            .collect();

        let schema = get_log_domain_metadata_schema().clone();

        let dm_actions_iter: Vec<_> = dm_actions_vec
            .iter()
            .cloned()
            .map(|dm| dm.into_engine_data(schema.clone(), engine))
            .collect();

        Ok((Box::new(dm_actions_iter.into_iter()), dm_actions_vec))
    }
}
