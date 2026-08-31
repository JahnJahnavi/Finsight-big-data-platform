// ===========================================================================
// FinSight - Phase 10: validate finsight.customers  (spec section 10)
//
//   docker exec finsight-mongodb mongosh \
//     "mongodb://<user>:<pw>@localhost:27017/finsight?authSource=admin" \
//     --quiet --file /opt/finsight/mongodb/validation.js
//
// (mongodb/ is mounted read-only at /opt/finsight/mongodb - see docker-compose.yml)
//
// Exits non-zero (quit(1)) if any check fails, so it can gate a pipeline.
// Checks: collection non-empty; customerId unique + present on every doc;
//         the compound (customerId, segment) index exists; customer counts by
//         segment cover exactly the five expected segments and sum to the total.
// ===========================================================================
const dbName = (typeof MONGO_DB !== "undefined" && MONGO_DB) ? MONGO_DB : "finsight";
const collName =
  (typeof MONGO_COLLECTION !== "undefined" && MONGO_COLLECTION) ? MONGO_COLLECTION : "customers";

// keep in sync with mongodb/segments.py
const EXPECTED_SEGMENTS = ["Premium", "Standard", "Basic", "Private Banking", "Student"];

const coll = db.getSiblingDB(dbName).getCollection(collName);
const problems = [];

// --- collection populated ---
const total = coll.countDocuments({});
if (total === 0) {
  print(`[validation] FAIL: ${dbName}.${collName} is empty`);
  quit(1);
}

// --- customerId preserved on every doc, unique, string ---
const missingId = coll.countDocuments({ $or: [{ customerId: { $exists: false } }, { customerId: null }] });
if (missingId > 0) problems.push(`${missingId} document(s) have no customerId`);

const nonString = coll.countDocuments({ customerId: { $exists: true, $not: { $type: "string" } } });
if (nonString > 0) problems.push(`${nonString} customerId value(s) are not strings (join key must be exact)`);

const distinctId = coll.distinct("customerId").length;
if (distinctId !== total) problems.push(`customerId not unique: ${distinctId} distinct vs ${total} docs`);

// --- required compound index (customerId, segment) ---
const idx = coll.getIndexes();
const hasCompound = idx.some((ix) => {
  const k = Object.keys(ix.key);
  return k.length === 2 && k[0] === "customerId" && k[1] === "segment";
});
if (!hasCompound) problems.push("missing required compound index { customerId: 1, segment: 1 } - run mongodb/indexes.js");

const hasUniqueId = idx.some((ix) => ix.unique && Object.keys(ix.key).join(",") === "customerId");
if (!hasUniqueId) problems.push("missing unique index on customerId - run mongodb/indexes.js");

// --- customer counts by segment ---
const bySeg = {};
coll.aggregate([{ $group: { _id: "$segment", n: { $sum: 1 } } }, { $sort: { n: -1 } }]).forEach((r) => {
  bySeg[r._id === null ? "<null>" : r._id] = r.n;
});

print(`\n  ${dbName}.${collName} - ${total} customers`);
print("  " + "-".repeat(40));
EXPECTED_SEGMENTS.forEach((s) => print(`  ${s.padEnd(18)} ${String(bySeg[s] || 0).padStart(6)}`));
Object.keys(bySeg)
  .filter((s) => EXPECTED_SEGMENTS.indexOf(s) === -1)
  .forEach((s) => print(`  ${(s + " (UNEXPECTED)").padEnd(18)} ${String(bySeg[s]).padStart(6)}`));
print("  " + "-".repeat(40));

const missingSeg = EXPECTED_SEGMENTS.filter((s) => !(bySeg[s] > 0));
if (missingSeg.length) problems.push(`missing / empty segment(s): ${JSON.stringify(missingSeg)}`);

const unexpectedSeg = Object.keys(bySeg).filter((s) => EXPECTED_SEGMENTS.indexOf(s) === -1);
if (unexpectedSeg.length) problems.push(`unexpected segment value(s): ${JSON.stringify(unexpectedSeg)}`);

const summed = Object.values(bySeg).reduce((a, b) => a + b, 0);
if (summed !== total) problems.push(`segment counts sum to ${summed}, expected ${total} (NULL / missing segment)`);

// --- verdict ---
print("");
if (problems.length) {
  problems.forEach((p) => print(`[validation] FAIL: ${p}`));
  quit(1);
}
print(`[validation] PASS: ${total} customers, ${EXPECTED_SEGMENTS.length} segments, customerId unique + indexed`);
