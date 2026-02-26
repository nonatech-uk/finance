const BASE_URL = '/api/v1'

export class ApiError extends Error {
  constructor(public status: number, message: string) {
    super(message)
  }
}

export async function apiFetch<T>(path: string, options?: RequestInit): Promise<T> {
  const res = await fetch(`${BASE_URL}${path}`, {
    credentials: 'include',
    headers: { 'Content-Type': 'application/json', ...options?.headers },
    ...options,
  })
  if (res.status === 401) {
    window.location.reload()
    throw new ApiError(401, 'Not authenticated')
  }
  if (!res.ok) {
    const body = await res.text()
    throw new ApiError(res.status, body)
  }
  return res.json()
}
