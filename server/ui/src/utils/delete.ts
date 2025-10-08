import { apiClient } from './loaders'

async function apiDelete<T>(url: string): Promise<T> {
  try {
    const response = await apiClient.delete<T>(url)
    return response.data
  } catch (error) {
    console.error(`Error fetching ${url}:`, error)
    throw error
  }
}

export async function deleteApplication({
  namespace,
  application,
}: {
  namespace: string
  application: string
}): Promise<boolean> {
  try {
    const response = await apiDelete(
      `/v1/namespaces/${namespace}/applications/${application}`
    )
    if (response === 200) {
      return true
    } else {
      return false
    }
  } catch {
    return false
  }
}
