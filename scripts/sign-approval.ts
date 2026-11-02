// Sign an approval (Agent extension §5, NXA-6).
//   npx tsx scripts/sign-approval.ts <private-key.pem> "<string from GET /x-nexus/requests/{id}>"
// Prints the base64url signature to send as { decision, signature } on the request.
// Keys: openssl genpkey -algorithm ed25519 -out me.pem && openssl pkey -in me.pem -pubout -out me.pub.pem
import { readFileSync } from "node:fs";
import { createPrivateKey, sign } from "node:crypto";
const [keyFile, text] = process.argv.slice(2);
if (!keyFile || !text) { console.error("usage: sign-approval.ts <private-key.pem> <signing string>"); process.exit(2); }
console.log(sign(null, Buffer.from(text), createPrivateKey(readFileSync(keyFile, "utf8"))).toString("base64url"));
