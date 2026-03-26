const fs = require('fs');
const forge = require('node-forge');

function gen(subjectCN = 'HPAY-68357') {
  const keys = forge.pki.rsa.generateKeyPair(2048);
  const cert = forge.pki.createCertificate();
  cert.publicKey = keys.publicKey;
  cert.serialNumber = String(Math.floor(Math.random() * 1e16));
  const now = new Date();
  cert.validity.notBefore = new Date(now.getTime() - 60000);
  cert.validity.notAfter = new Date(now.getTime() + 365 * 24 * 3600 * 1000);
  const attrs = [{ name: 'commonName', value: subjectCN }];
  cert.setSubject(attrs);
  cert.setIssuer(attrs);
  cert.setExtensions([{ name: 'basicConstraints', cA: false }]);
  cert.sign(keys.privateKey, forge.md.sha256.create());
  const pemKey = forge.pki.privateKeyToPem(keys.privateKey);
  const pemCert = forge.pki.certificateToPem(cert);
  fs.writeFileSync('D:/bot-tele-binh/new_private_68357.pem', pemKey, 'utf8');
  fs.writeFileSync('D:/bot-tele-binh/new_public_68357.cer', pemCert, 'utf8');
  const derBytes = forge.asn1.toDer(forge.pki.certificateToAsn1(cert)).getBytes();
  const hash = forge.md.sha256.create();
  hash.update(derBytes);
  const hex = hash.digest().toHex().toUpperCase().match(/.{1,2}/g).join(':');
  process.stdout.write(`Wrote D:/bot-tele-binh/new_private_68357.pem\n`);
  process.stdout.write(`Wrote D:/bot-tele-binh/new_public_68357.cer\n`);
  process.stdout.write(`Cert SHA256 fingerprint: ${hex}\n`);
}

gen(process.argv[2] || 'HPAY-68357');
