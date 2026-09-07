-- AP-2: address-write gated on NULL. The version already carries an address from the object's
-- create, so on the append hot path this is a no-op; it only ever fills a legacy NULL row. Keeps the
-- drain's "address is monotonic, set once" contract while removing a redundant UPDATE per append.
--
-- billing_bypass ($4) rides the same gate deliberately: on a version that already has an address
-- the create already recorded the verdict, and an append must not be able to flip it afterwards.
UPDATE object_versions
SET address = $3,
    billing_bypass = $4
WHERE object_id = $1 AND object_version = $2 AND address IS NULL;
