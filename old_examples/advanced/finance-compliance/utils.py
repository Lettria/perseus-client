"""
SPARQL and Cypher queries for CSRD compliance demo.
See the notebook for usage examples.
"""

# =============================================================================
# SPARQL QUERIES (for RDF exploration)
# =============================================================================

SPARQL_COUNT_BY_TYPE = """
SELECT ?type (COUNT(?entity) as ?count)
WHERE {
    ?entity a ?type .
}
GROUP BY ?type
ORDER BY DESC(?count)
"""

SPARQL_GHG_EMISSIONS = """
SELECT ?label ?value ?unit ?year
WHERE {
    ?metric a ?type .
    FILTER(CONTAINS(STR(?type), "GHGEmissionsMetric"))
    ?metric rdfs:label ?label .
    OPTIONAL { ?metric ont:hasValue ?value }
    OPTIONAL { ?metric ont:hasUnit ?unit }
    OPTIONAL { ?metric ont:hasYear ?year }
    FILTER(CONTAINS(STR(?label), "Scope 1") ||
           CONTAINS(STR(?label), "Scope 2") ||
           CONTAINS(STR(?label), "Scope 3 Emissions 2024"))
}
ORDER BY ?label
LIMIT 3
"""

# =============================================================================
# CYPHER QUERIES (for Neo4j compliance checks)
# =============================================================================

CYPHER_GHG_EMISSIONS = """
MATCH (c:Company {label: $name})-[:hasMetric]->(m:GHGEmissionsMetric)
RETURN count(m) > 0 AS present
"""

CYPHER_CARBON_INTENSITY = """
MATCH (c:Company {label: $name})-[:hasMetric]->(m:CarbonIntensityMetric)
RETURN count(m) > 0 AS present
"""

CYPHER_NET_ZERO = """
MATCH (c:Company {label: $name})-[:hasCommitment|hasTarget]->(t)
WHERE t.label =~ '(?i).*net.?zero.*' OR t.hasTargetYear >= '2040'
RETURN count(t) > 0 AS present
"""

CYPHER_STRATEGIC_PROGRAM = """
MATCH (c:Company {label: $name})-[:hasProgram]->(p:StrategicProgram)-[:hasPillar]->(pillar)
RETURN count(pillar) > 0 AS present
"""

CYPHER_CLIMATE_RISKS = """
MATCH (c:Company {label: $name})-[:hasRisk]->(r)
WHERE r:PhysicalRisk OR r:TransitionRisk OR r:EnvironmentalRisk
RETURN count(r) > 0 AS present
"""

# =============================================================================
# HELPER FUNCTIONS
# =============================================================================

KEY_ENTITY_TYPES = [
    "GHGEmissionsMetric",
    "CarbonIntensityMetric",
    "ReductionTarget",
    "StrategicProgram",
    "StrategicPillar",
    "PhysicalRisk",
    "TransitionRisk",
    "Commitment",
]


def explore_rdf_entities(ttl_path: str) -> dict:
    """Load RDF file and return summary of key entities."""
    from rdflib import Graph, RDF

    g = Graph()
    g.parse(ttl_path, format="turtle")

    # Count by type
    type_counts = {}
    for row in g.query(SPARQL_COUNT_BY_TYPE):
        type_name = str(row["type"]).split("#")[-1].split("/")[-1]
        if type_name in KEY_ENTITY_TYPES:
            type_counts[type_name] = int(row["count"])

    # Sample GHG emissions
    ghg_samples = []
    for row in g.query(SPARQL_GHG_EMISSIONS):
        ghg_samples.append(
            {
                "label": str(row.label),
                "value": str(row.value),
                "unit": str(row.unit),
                "year": str(row.year),
            }
        )

    return {
        "type_counts": type_counts,
        "ghg_samples": ghg_samples,
        "total_triples": len(g),
        "total_entities": len(set(g.subjects(RDF.type, None))),
    }


def check_esrs_e1_compliance(tx, company_name: str) -> dict:
    """Check ESRS E1 indicators for a company."""
    results = {}

    # E1-6: GHG emissions
    r = tx.run(CYPHER_GHG_EMISSIONS, name=company_name).single()
    results["E1-6: GHG emissions disclosed"] = r["present"] if r else False

    # E1-5: Carbon intensity
    r = tx.run(CYPHER_CARBON_INTENSITY, name=company_name).single()
    results["E1-5: Carbon intensity disclosed"] = r["present"] if r else False

    # E1-4: Net-Zero
    r = tx.run(CYPHER_NET_ZERO, name=company_name).single()
    results["E1-4: Net-Zero target set"] = r["present"] if r else False

    # Strategic program
    r = tx.run(CYPHER_STRATEGIC_PROGRAM, name=company_name).single()
    results["Strategic program with pillars"] = r["present"] if r else False

    # Climate risks
    r = tx.run(CYPHER_CLIMATE_RISKS, name=company_name).single()
    results["Climate risks with description"] = r["present"] if r else False

    return results


def print_compliance_report(results: dict, company_name: str):
    """Pretty-print compliance results."""
    print(f"\n🏢 {company_name}")

    quantitative = {k: v for k, v in results.items() if k.startswith("E1-")}
    narrative = {k: v for k, v in results.items() if not k.startswith("E1-")}

    print("  Quantitative (structured numbers):")
    for indicator, compliant in quantitative.items():
        print(f"    {'✓' if compliant else '✗'} {indicator}")

    print("  Narrative (contextual information):")
    for indicator, compliant in narrative.items():
        print(f"    {'✓' if compliant else '✗'} {indicator}")

    passed = sum(results.values())
    total = len(results)
    print(f"\n  {passed}/{total} indicators ({int(passed / total * 100)}%)")
