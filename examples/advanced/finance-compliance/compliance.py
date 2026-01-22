import logging
import os
from neo4j import GraphDatabase
from dotenv import load_dotenv
from utils import check_esrs_e1_compliance, print_compliance_report

logging.basicConfig(level=logging.INFO)
load_dotenv()


def main():
    """
    Connects to Neo4j and runs the ESRS E1 compliance checks.
    """
    uri = os.getenv("NEO4J_URI", "bolt://localhost:7687")
    user = os.getenv("NEO4J_USER", "neo4j")
    password = os.getenv("NEO4J_PASSWORD", "password")

    try:
        driver = GraphDatabase.driver(uri, auth=(user, password))
        logging.info("Successfully connected to Neo4j.")
    except Exception as e:
        logging.error(f"Failed to connect to Neo4j: {e}")
        return

    print("\n📋 CSRD ESRS E1 Compliance Verification")
    print("=" * 70)

    companies = ["EcoSteel Industries", "TechGreen Solutions"]

    with driver.session() as session:
        for company in companies:
            try:
                results = session.execute_read(check_esrs_e1_compliance, company)
                print_compliance_report(results, company)
            except Exception as e:
                logging.error(f"Could not run compliance check for {company}: {e}")

    driver.close()
    print("\n" + "=" * 70)


if __name__ == "__main__":
    main()
