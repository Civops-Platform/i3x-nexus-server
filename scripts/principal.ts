// Make a principal entry: prints the JSON line (with the token's SHA-256) and the
// token itself once. Store the token where the client keeps secrets; only the hash
// goes in the principals file.
//
//   npx tsx scripts/principal.ts <id> <person|agent|service> <scope,scope> [ownedNamespace,...]
import { randomBytes } from "node:crypto";
import { sha256 } from "../src/core/auth";
const [id, kind, scopes, owns] = process.argv.slice(2);
if (!id || !kind || !scopes) { console.error("usage: principal.ts <id> <kind> <scopes> [owns]"); process.exit(2); }
const token = randomBytes(24).toString("base64url");
console.log(JSON.stringify({ id, kind, scopes: scopes.split(","), owns: owns ? owns.split(",") : [], tokenSha256: sha256(token) }));
console.error(`token for ${id}: ${token}`);
