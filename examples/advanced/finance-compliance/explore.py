import logging
import glob
from dotenv import load_dotenv
from utils import explore_rdf_entities

logging.basicConfig(level=logging.INFO)
load_dotenv()


def main():
    """
    Explores the generated RDF files and prints a summary of extracted entities.
    """
    ttl_files = glob.glob("output/**/*.ttl", recursive=True)

    if not ttl_files:
        logging.warning("No .ttl files found in the output directory. Please run `index.py` first.")
        return

    print("\n" + "=" * 70)
    print("EXTRACTED ENTITIES SUMMARY")
    print("=" * 70)

    for ttl_file in ttl_files:
        print(f"\n📄 Processing RDF file: {ttl_file}")
        try:
            summary = explore_rdf_entities(ttl_file)

            print("\nKey climate reporting entities:")
            for type_name, count in summary["type_counts"].items():
                print(f"  • {type_name}: {count}")

            total_key_entities = sum(summary["type_counts"].values())
            print(f"\n({total_key_entities} key entities shown, {summary['total_entities']} total entities extracted)")

            print("\n" + "=" * 70)
            print("SAMPLE: GHG EMISSIONS METRICS (Scope 1, 2, 3)")
            print("=" * 70)

            if summary["ghg_samples"]:
                for sample in summary["ghg_samples"]:
                    print(
                        f"  • {sample['label']}: {sample['value']} {sample['unit']} ({sample['year']})"
                    )
            else:
                print("  No GHG Emissions Metrics samples found.")

            print("\n" + "=" * 70)
            print(f"✓ Total triples in graph: {summary['total_triples']}")
            print("✓ The ontology guided Perseus to extract structured, queryable entities")

        except Exception as e:
            logging.error(f"Error processing {ttl_file}: {e}")

    print("\n" + "=" * 70)


if __name__ == "__main__":
    main()
