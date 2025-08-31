export interface SecretSummary {
  key: string;
  masked_value: string;
}

export async function createSecret(
  key: string,
  value: string,
): Promise<boolean> {
  try {
    const response = await fetch('http://localhost:3000/api/secrets', {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
      },
      body: JSON.stringify({ key, value }),
    });

    return response.ok;
  } catch (error) {
    console.error('Failed to create secret:', error);
    return false;
  }
}

export async function updateSecret(
  key: string,
  value: string,
): Promise<boolean> {
  try {
    const response = await fetch(`http://localhost:3000/api/secrets/${key}`, {
      method: 'PUT',
      headers: {
        'Content-Type': 'application/json',
      },
      body: JSON.stringify({ value }),
    });

    return response.ok;
  } catch (error) {
    console.error('Failed to update secret:', error);
    return false;
  }
}

export async function deleteSecret(key: string): Promise<boolean> {
  try {
    const response = await fetch(`http://localhost:3000/api/secrets/${key}`, {
      method: 'DELETE',
    });

    return response.ok;
  } catch (error) {
    console.error('Failed to delete secret:', error);
    return false;
  }
}

export async function getSecretInfo(
  key: string,
): Promise<SecretSummary | null> {
  try {
    const response = await fetch(`http://localhost:3000/api/secrets/${key}`);

    if (response.ok) {
      return await response.json();
    }
    return null;
  } catch (error) {
    console.error('Failed to get secret info:', error);
    return null;
  }
}

export async function listSecrets(): Promise<SecretSummary[]> {
  try {
    const response = await fetch('http://localhost:3000/api/secrets');

    if (response.ok) {
      return await response.json();
    }
    return [];
  } catch (error) {
    console.error('Failed to list secrets:', error);
    return [];
  }
}

export async function generateUniqueSecretKey(
  connectionName: string,
  connectionType: string,
  fieldType: string,
): Promise<string> {
  try {
    const response = await fetch(
      'http://localhost:3000/api/secrets/generate-key',
      {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({
          connection_name: connectionName,
          connection_type: connectionType,
          field_type: fieldType,
        }),
      },
    );

    if (response.ok) {
      const result = await response.json();
      return result.key;
    }
    throw new Error('Failed to generate unique secret key');
  } catch (error) {
    console.error('Failed to generate unique secret key:', error);
    throw error;
  }
}

export function extractSecretKey(secretReference: string): string | null {
  const match = secretReference.match(/^\$\{SECRET_([A-Z_][A-Z0-9_]*)\}$/);
  return match ? match[1] : null;
}

export function createSecretReference(key: string): string {
  return `\${SECRET_${key}}`;
}

export function isSecretReference(value: string): boolean {
  return /^\$\{SECRET_[A-Z_][A-Z0-9_]*\}$/.test(value);
}

export async function findUniqueSecretKey(baseKey: string): Promise<string> {
  let candidate = baseKey;
  let secrets = await listSecrets();
  let existingKeys = new Set(secrets.map((s) => s.key));

  if (!existingKeys.has(candidate)) {
    return candidate;
  }

  for (let i = 2; i <= 999; i++) {
    candidate = `${baseKey}_${i}`;
    if (!existingKeys.has(candidate)) {
      return candidate;
    }
  }

  throw new Error(`Unable to find unique secret key for: ${baseKey}`);
}

export function extractSecretsFromConnection(connectionConfig: any): string[] {
  const secretKeys: string[] = [];

  if (connectionConfig.type === 'mysql' && connectionConfig.password) {
    const key = extractSecretKey(connectionConfig.password);
    if (key) secretKeys.push(key);
  }

  if (connectionConfig.type === 'postgresql' && connectionConfig.password) {
    const key = extractSecretKey(connectionConfig.password);
    if (key) secretKeys.push(key);
  }

  if (connectionConfig.type === 's3' && connectionConfig.secret_access_key) {
    const key = extractSecretKey(connectionConfig.secret_access_key);
    if (key) secretKeys.push(key);
  }

  return secretKeys;
}

export async function deleteConnectionSecrets(
  connectionConfig: any,
): Promise<void> {
  const secretKeys = extractSecretsFromConnection(connectionConfig);

  for (const key of secretKeys) {
    try {
      await deleteSecret(key);
    } catch (error) {
      console.warn(`Failed to delete secret ${key}:`, error);
    }
  }
}
