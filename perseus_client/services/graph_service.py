
import logging
from typing import Optional, List, Dict

from ..models import KnowledgeGraph, Entity


class GraphService:
    def interlink(
        self,
        kbs: List["KnowledgeGraph"],
        interlinking_key_uri: str = "http://www.w3.org/2000/01/rdf-schema#label",
        immutable_properties: Optional[List[str]] = None,
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

        # By default, prevent merging entities with conflicting names.
        if immutable_properties is None:
            immutable_properties = ["name", "hasName", "hasFullName"]

        # Helper to resolve simple names to full URIs
        def _resolve_property_name(
            name: str, all_entities: List[Entity], namespaces: Dict[str, str]
        ) -> Optional[str]:
            if name.startswith("http://") or name.startswith("https://"):
                return name
            if ":" in name:
                prefix, local = name.split(":", 1)
                if prefix in namespaces:
                    return namespaces[prefix] + local

            # Search for a property URI that ends with the simple name
            for entity in all_entities:
                for prop_uri in entity.properties.keys():
                    if prop_uri.endswith(f"#{name}") or prop_uri.endswith(f"/{name}"):
                        return prop_uri
            logging.warning(f"Could not resolve property name '{name}' to a full URI.")
            return None

        # --- Main interlink logic ---

        # Initialize the new KG
        merged_kg = KnowledgeGraph()

        entity_map: Dict[str, Entity] = {}
        uri_redirects: Dict[str, str] = {}

        # Aggregate namespaces and entities from all KGs for resolution
        all_entities = [entity for kg in kbs for entity in kg.entities]
        for kg in kbs:
            merged_kg.namespaces.update(kg.namespaces)

        # Resolve immutable properties to full URIs
        resolved_immutable_properties = []
        if immutable_properties:
            for prop_name in immutable_properties:
                resolved_uri = _resolve_property_name(
                    prop_name, all_entities, merged_kg.namespaces
                )
                if resolved_uri:
                    resolved_immutable_properties.append(resolved_uri)

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

                    # --- Conflict Check for Immutable Properties ---
                    has_conflict = False
                    for prop_uri in resolved_immutable_properties:
                        if (
                            prop_uri in existing_entity.properties
                            and prop_uri in entity.properties
                            and existing_entity.properties[prop_uri].value
                            != entity.properties[prop_uri].value
                        ):
                            logging.warning(
                                f"Merge conflict on immutable property '{prop_uri}' for "
                                f"entity with key '{key_value}'. Values: "
                                f"'{existing_entity.properties[prop_uri].value}' vs "
                                f"'{entity.properties[prop_uri].value}'. "
                                f"Entity '{entity.uri}' will not be merged."
                            )
                            has_conflict = True
                            break

                    if has_conflict:
                        merged_kg.entities.append(entity)
                        continue
                    # --- End of Conflict Check ---

                    uri_redirects[entity.uri] = existing_entity.uri

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
                if relation.source_uri in uri_redirects:
                    source_uri = uri_redirects[relation.source_uri]
                else:
                    source_uri = relation.source_uri

                if relation.target_uri in uri_redirects:
                    target_uri = uri_redirects[relation.target_uri]
                else:
                    target_uri = relation.target_uri

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

        return merged_kg
