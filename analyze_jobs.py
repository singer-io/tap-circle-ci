"""Analyze jobs data from two sync outputs by primary key (id, _workflow_id)."""
import json
import sys


def load_jobs(filepath):
    """Load job records from a Singer output file."""
    records = []
    with open(filepath, "r") as f:
        for line in f:
            msg = json.loads(line)
            if msg.get("type") == "RECORD" and msg.get("stream") == "jobs":
                records.append(msg["record"])
    return records


def extract_pks(records):
    """Extract primary key tuples (id, _workflow_id) from records."""
    return {(r["id"], r["_workflow_id"]) for r in records}


def main():
    file1 = "output.json"   # start_date_1: 2021-09-09
    file2 = "output2.json"  # start_date_2: 2022-06-20

    records_1 = load_jobs(file1)
    records_2 = load_jobs(file2)

    pks_1 = extract_pks(records_1)
    pks_2 = extract_pks(records_2)

    only_in_1 = pks_1 - pks_2
    only_in_2 = pks_2 - pks_1
    common = pks_1 & pks_2

    print("=" * 60)
    print(f"File 1 ({file1}): {len(records_1)} records, {len(pks_1)} unique PKs")
    print(f"File 2 ({file2}): {len(records_2)} records, {len(pks_2)} unique PKs")
    print("=" * 60)
    print(f"Common PKs:        {len(common)}")
    print(f"Only in file 1:    {len(only_in_1)}")
    print(f"Only in file 2:    {len(only_in_2)}")
    print("=" * 60)

    # Subset checks
    print(f"\npks_2 ⊆ pks_1?  {pks_2.issubset(pks_1)}")
    print(f"pks_1 ⊆ pks_2?  {pks_1.issubset(pks_2)}")
    print(f"pks_1 == pks_2?  {pks_1 == pks_2}")

    if only_in_1:
        print(f"\n--- Sample PKs only in file 1 (up to 10) ---")
        for pk in list(only_in_1)[:10]:
            print(f"  id={pk[0]}, _workflow_id={pk[1]}")

    if only_in_2:
        print(f"\n--- Sample PKs only in file 2 (up to 10) ---")
        for pk in list(only_in_2)[:10]:
            print(f"  id={pk[0]}, _workflow_id={pk[1]}")

    # Check if records with same PK have same data
    if common:
        records_by_pk_1 = {(r["id"], r["_workflow_id"]): r for r in records_1}
        records_by_pk_2 = {(r["id"], r["_workflow_id"]): r for r in records_2}
        mismatches = 0
        for pk in common:
            if records_by_pk_1[pk] != records_by_pk_2[pk]:
                mismatches += 1
                if mismatches <= 3:
                    print(f"\n--- Data mismatch for PK {pk} ---")
                    print(f"  File 1: {json.dumps(records_by_pk_1[pk], indent=2)}")
                    print(f"  File 2: {json.dumps(records_by_pk_2[pk], indent=2)}")
        print(f"\nData mismatches among common PKs: {mismatches}/{len(common)}")


if __name__ == "__main__":
    main()
