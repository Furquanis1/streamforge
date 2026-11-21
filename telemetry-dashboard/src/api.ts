export const API = {
  edge: import.meta.env.VITE_EDGE_API_URL || 'http://localhost:8083',
  fog: import.meta.env.VITE_FOG_API_URL || 'http://localhost:8082',
  cloud: import.meta.env.VITE_CLOUD_API_URL || 'http://localhost:8081',
}

export async function getJson<T>(baseUrl: string, path: string): Promise<T> {
  const response = await fetch(`${baseUrl}${path}`)
  if (!response.ok) throw new Error(`HTTP ${response.status}`)
  return response.json() as Promise<T>
}

export async function postJson<T>(baseUrl: string, path: string, body: unknown): Promise<T> {
  const response = await fetch(`${baseUrl}${path}`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(body),
  })
  if (!response.ok) throw new Error(`HTTP ${response.status}`)
  return response.json() as Promise<T>
}
