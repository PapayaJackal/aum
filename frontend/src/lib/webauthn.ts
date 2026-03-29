/** Base64url encoding/decoding and browser WebAuthn API wrappers. */

function base64urlToBytes(base64url: string): Uint8Array {
  const base64 = base64url.replace(/-/g, "+").replace(/_/g, "/");
  const pad = base64.length % 4;
  const padded = pad ? base64 + "=".repeat(4 - pad) : base64;
  const binary = atob(padded);
  const bytes = new Uint8Array(binary.length);
  for (let i = 0; i < binary.length; i++) {
    bytes[i] = binary.charCodeAt(i);
  }
  return bytes;
}

function bytesToBase64url(bytes: ArrayBuffer): string {
  const arr = new Uint8Array(bytes);
  let binary = "";
  for (const b of arr) {
    binary += String.fromCharCode(b);
  }
  return btoa(binary).replace(/\+/g, "-").replace(/\//g, "_").replace(/=+$/, "");
}

/**
 * Call navigator.credentials.create() with options from the server.
 * Returns a JSON-serializable credential object ready to POST back.
 */
export async function createPasskey(optionsJson: string): Promise<object> {
  const options = JSON.parse(optionsJson);

  // Decode base64url fields the browser expects as ArrayBuffers
  options.challenge = base64urlToBytes(options.challenge);
  options.user.id = base64urlToBytes(options.user.id);
  if (options.excludeCredentials) {
    for (const cred of options.excludeCredentials) {
      cred.id = base64urlToBytes(cred.id);
    }
  }

  const credential = (await navigator.credentials.create({
    publicKey: options,
  })) as PublicKeyCredential;

  if (!credential) {
    throw new Error("Passkey creation was cancelled");
  }

  const response = credential.response as AuthenticatorAttestationResponse;
  return {
    id: credential.id,
    rawId: bytesToBase64url(credential.rawId),
    type: credential.type,
    response: {
      attestationObject: bytesToBase64url(response.attestationObject),
      clientDataJSON: bytesToBase64url(response.clientDataJSON),
      transports: response.getTransports?.() ?? [],
    },
  };
}

/**
 * Call navigator.credentials.get() with options from the server.
 * Returns a JSON-serializable credential object ready to POST back.
 */
export async function verifyPasskey(optionsJson: string): Promise<object> {
  const options = JSON.parse(optionsJson);

  options.challenge = base64urlToBytes(options.challenge);
  if (options.allowCredentials) {
    for (const cred of options.allowCredentials) {
      cred.id = base64urlToBytes(cred.id);
    }
  }

  const credential = (await navigator.credentials.get({
    publicKey: options,
  })) as PublicKeyCredential;

  if (!credential) {
    throw new Error("Passkey verification was cancelled");
  }

  const response = credential.response as AuthenticatorAssertionResponse;
  return {
    id: credential.id,
    rawId: bytesToBase64url(credential.rawId),
    type: credential.type,
    response: {
      authenticatorData: bytesToBase64url(response.authenticatorData),
      clientDataJSON: bytesToBase64url(response.clientDataJSON),
      signature: bytesToBase64url(response.signature),
      userHandle: response.userHandle ? bytesToBase64url(response.userHandle) : null,
    },
  };
}
