// ===========================================================================
// FinSight - Phase 10: indexes for finsight.customers  (spec section 10)
//
//   docker exec finsight-mongodb mongosh \
//     "mongodb://<user>:<pw>@localhost:27017/finsight?authSource=admin" \
//     --quiet --file /opt/finsight/mongodb/indexes.js
//
// (mongodb/ is mounted read-only at /opt/finsight/mongodb - see docker-compose.yml)
// mongodb/import_customers.sh runs this automatically after a load.
//
// Idempotent - createIndex() is a no-op when the index already exists.
// ===========================================================================
const dbName = (typeof MONGO_DB !== "undefined" && MONGO_DB) ? MONGO_DB : "finsight";
const collName =
  (typeof MONGO_COLLECTION !== "undefined" && MONGO_COLLECTION) ? MONGO_COLLECTION : "customers";

const target = db.getSiblingDB(dbName);
const coll = target.getCollection(collName);

if (coll.countDocuments({}, { limit: 1 }) === 0) {
  print(`[indexes] WARNING: ${dbName}.${collName} is empty - run mongodb/import_customers.sh first`);
}

// 1. customerId is THE join key (-> Hive / Spark / Neo4j). Preserve it exactly
//    and forbid duplicates.
const ix1 = coll.createIndex({ customerId: 1 }, { unique: true, name: "ux_customerId" });

// 2. Required compound index: customerId + segment. Covers the segment-scoped
//    per-customer lookups the BI / Alteryx joins do.
const ix2 = coll.createIndex(
  { customerId: 1, segment: 1 },
  { name: "ix_customerId_segment" }
);

// 3. Plain segment index - the segment-count validation and segment dashboards
//    group by this field.
const ix3 = coll.createIndex({ segment: 1 }, { name: "ix_segment" });

print(`[indexes] ${dbName}.${collName}: ${ix1}, ${ix2}, ${ix3}`);
print("[indexes] current indexes:");
coll.getIndexes().forEach((ix) => print("  " + ix.name + "  " + JSON.stringify(ix.key)));
