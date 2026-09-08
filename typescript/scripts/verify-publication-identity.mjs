import { PublicationIdentitySchema } from "@saqi/precedent-iso";

const chunks = [];
process.stdin.setEncoding("utf8");
for await (const chunk of process.stdin) chunks.push(chunk);
const input = chunks.join("");
PublicationIdentitySchema.parse(JSON.parse(input));
