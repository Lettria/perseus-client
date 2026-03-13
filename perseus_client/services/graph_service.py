import logging
from typing import Optional, List, Dict

from ..models import KnowledgeGraph, Entity

logger = logging.getLogger(__name__)


class GraphService:
    def interlink(
        self,
        kbs: List["KnowledgeGraph"],
        interlinking_key_uri: str = "http://www.w3.org/2000/01/rdf-schema#label",
        immutable_properties: Optional[List[str]] = None,
        merge_properties_on_conflict: bool = False,
    ) -> "KnowledgeGraph":
        """
        Merges multiple KnowledgeGraph objects into a single, deduplicated graph,
        with an option to prevent merging if certain properties conflict.

        Args:
            kbs: A list of KnowledgeGraph objects to merge.
            interlinking_key_uri: The URI of the property to use for deduplicating
                                  entities (e.g., rdfs:label).
            immutable_properties: A list of property URIs or simple names (e.g., "label")
                                  that must not conflict. If a duplicate entity has a
                                  conflicting value for one of these properties,
                                  it will not be merged. Defaults to a list of common
                                  name properties.
        Returns:
            A new, unified KnowledgeGraph.
        """
        if not kbs:
            return KnowledgeGraph()

        logger.debug(f"Starting interlink process for {len(kbs)} knowledge graphs.")
        total_entities_before = sum(len(kg.entities) for kg in kbs)
        total_relations_before = sum(len(kg.relations) for kg in kbs)
        logger.debug(
            f"Total entities before merge: {total_entities_before}, Total relations before merge: {total_relations_before}"
        )

        if immutable_properties is None:
            immutable_properties = ["name", "hasName", "hasFullName"]

        def _resolve_property_name(
            name: str, all_entities: List[Entity], namespaces: Dict[str, str]
        ) -> Optional[str]:
            if name.startswith("http://") or name.startswith("https://"):
                return name
            if ":" in name:
                prefix, local = name.split(":", 1)
                if prefix in namespaces:
                    return namespaces[prefix] + local

            # 3. If it's a short name (no prefix, no http/https), try to construct full URIs
            #    using known namespaces and check if they exist as properties.
            for prefix, uri_base in namespaces.items():
                # Try with # separator
                potential_uri_hash = uri_base + "#" + name
                # Check if this potential URI exists in any entity's properties
                for entity in all_entities:
                    if potential_uri_hash in entity.properties:
                        logger.debug(
                            f"Resolved property name '{name}' to '{potential_uri_hash}' using namespace '{prefix}' and '#' separator."
                        )
                        return potential_uri_hash

                # Try with / separator
                potential_uri_slash = uri_base + "/" + name
                # Check if this potential URI exists in any entity's properties
                for entity in all_entities:
                    if potential_uri_slash in entity.properties:
                        logger.debug(
                            f"Resolved property name '{name}' to '{potential_uri_slash}' using namespace '{prefix}' and '/' separator."
                        )
                        return potential_uri_slash

            # 4. Fallback to suffix matching (existing logic) as a last resort.
            #    This might catch properties that don't follow strict namespace+localName patterns
            #    but still have the short name at the end of their URI.
            for entity in all_entities:
                for prop_uri in entity.properties.keys():
                    if prop_uri.endswith(f"#{name}") or prop_uri.endswith(f"/{name}"):
                        logger.debug(
                            f"Resolved property name '{name}' via suffix match to '{prop_uri}'."
                        )
                        return prop_uri

            logger.debug(
                f"Could not resolve property name '{name}' to a full URI, returning original name for fallback comparison."
            )
            return name

        merged_kg = KnowledgeGraph()
        entity_map: Dict[str, Entity] = {}
        uri_redirects: Dict[str, str] = {}
        all_entities = [entity for kg in kbs for entity in kg.entities]
        for kg in kbs:
            merged_kg.namespaces.update(kg.namespaces)

        resolved_immutable_properties = []
        if immutable_properties:
            logger.debug(f"Using immutable properties: {immutable_properties}")
            for prop_name in immutable_properties:
                resolved_uri = _resolve_property_name(
                    prop_name, all_entities, merged_kg.namespaces
                )
                if resolved_uri:
                    resolved_immutable_properties.append(resolved_uri)
            logger.debug(
                f"Resolved immutable properties to URIs: {resolved_immutable_properties}"
            )
        logger.debug(f"kbs: {kbs}")
        # 1. Iterate through all entities to identify duplicates and merge them
        for kg in kbs:
            for entity in kg.entities:
                key_value = None
                if interlinking_key_uri in entity.properties:
                    key_value = str(entity.properties[interlinking_key_uri].value)

                if key_value is None:
                    key_value = entity.uri
                if key_value in entity_map:
                    existing_entity = entity_map[key_value]
                    has_conflict = False
                    for prop_uri in resolved_immutable_properties:
                        if (
                            prop_uri in existing_entity.properties
                            and prop_uri in entity.properties
                            and existing_entity.properties[prop_uri].value
                            != entity.properties[prop_uri].value
                        ):
                            logger.warning(
                                f"Merge conflict on immutable property '{prop_uri}' for entity with key '{key_value}'. "
                                f"Existing value: '{existing_entity.properties[prop_uri].value}', "
                                f"New value: '{entity.properties[prop_uri].value}'. "
                                f"Entity '{entity.uri}' will not be merged into '{existing_entity.uri}'."
                            )
                            has_conflict = True
                            break
                        else:
                            logger.debug(
                                f"No conflict on immutable property '{prop_uri}' for entity with key '{key_value}'. "
                                f"Existing value: '{existing_entity.properties.get(prop_uri)}', "
                                f"New value: '{entity.properties.get(prop_uri)}'."
                                f"Entity properties: {entity.properties}, Existing entity properties: {existing_entity.properties}"
                            )

                    if has_conflict:
                        merged_kg.entities.append(entity)
                        continue

                    logger.debug(
                        f"Merging entity '{entity.uri}' into existing entity '{existing_entity.uri}' based on key '{key_value}'."
                    )
                    uri_redirects[entity.uri] = existing_entity.uri

                    # Merge properties, combining values into a list on conflict
                    if merge_properties_on_conflict:
                        for prop_uri, new_prop_obj in entity.properties.items():
                            if prop_uri not in existing_entity.properties:
                                existing_entity.properties[prop_uri] = new_prop_obj
                            else:
                                existing_prop_obj = existing_entity.properties[prop_uri]

                                # Skip if values are identical
                                if existing_prop_obj.value == new_prop_obj.value:
                                    continue

                                # Ensure the existing value is a list for merging
                                if not isinstance(existing_prop_obj.value, list):
                                    existing_prop_obj.value = [existing_prop_obj.value]

                                # Add new value(s), avoiding duplicates
                                if isinstance(new_prop_obj.value, list):
                                    for item in new_prop_obj.value:
                                        if item not in existing_prop_obj.value:
                                            existing_prop_obj.value.append(item)
                                else:
                                    if (
                                        new_prop_obj.value
                                        not in existing_prop_obj.value
                                    ):
                                        existing_prop_obj.value.append(
                                            new_prop_obj.value
                                        )
                    else:
                        # Original behavior: add property only if it doesn't exist
                        for prop_uri, prop_value in entity.properties.items():
                            if prop_uri not in existing_entity.properties:
                                existing_entity.properties[prop_uri] = prop_value

                    for type_uri in entity.types:
                        if type_uri not in existing_entity.types:
                            existing_entity.types.append(type_uri)
                else:
                    entity_map[key_value] = entity
                    merged_kg.entities.append(entity)

        # 2. Iterate through all relations and relink them
        for kg in kbs:
            for relation in kg.relations:
                source_uri = uri_redirects.get(relation.source_uri, relation.source_uri)
                target_uri = uri_redirects.get(relation.target_uri, relation.target_uri)

                if not any(
                    r.source_uri == source_uri
                    and r.target_uri == target_uri
                    and r.predicate == relation.predicate
                    for r in merged_kg.relations
                ):
                    updated_relation = relation.model_copy()
                    updated_relation.source_uri = source_uri
                    updated_relation.target_uri = target_uri
                    merged_kg.relations.append(updated_relation)

        logger.info(
            f"Interlink process complete. Merged graph has {len(merged_kg.entities)} entities and {len(merged_kg.relations)} relations."
        )
        return merged_kg
