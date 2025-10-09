import { apiClient } from './loaders'

export interface DeleteResponse {
  success: boolean
  message: string
  statusCode?: number
}

async function apiDelete<T>(url: string) {
  try {
    const response = await apiClient.delete<T>(url)
    return response
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
}): Promise<DeleteResponse> {
  try {
    const response = await apiDelete(
      `/v1/namespaces/${namespace}/applications/${application}`
    )
    if (response.status === 200) {
      return {
        success: true,
        message: 'application deleted successfully',
      }
    } else {
      return {
        success: false,
        message: 'unexpected response',
        statusCode: response.status,
      }
    }
  } catch (error: any) {
    // Handle different HTTP status codes
    if (error.response?.status === 400) {
      return {
        success: false,
        message: 'unable to delete application',
        statusCode: 400,
      }
    } else if (error.response?.status === 500) {
      return {
        success: false,
        message: 'internal server error',
        statusCode: 500,
      }
    } else {
      return {
        success: false,
        message: 'failed to delete application',
        statusCode: error.response?.status,
      }
    }
  }
}

export async function deleteApplicationRequest({
  namespace,
  application,
  requestId,
}: {
  namespace: string
  application: string
  requestId: string
}): Promise<DeleteResponse> {
  try {
    const response = await apiDelete(
      `/v1/namespaces/${namespace}/applications/${application}/requests/${requestId}`
    )

    if (response.status === 200) {
      return {
        success: true,
        message: 'request has been deleted',
      }
    } else {
      return {
        success: false,
        message: 'unexpected response',
        statusCode: response.status,
      }
    }
  } catch (error: any) {
    // Handle different HTTP status codes
    if (error.response?.status === 404) {
      return {
        success: false,
        message: 'request not found',
        statusCode: 404,
      }
    } else if (error.response?.status === 500) {
      return {
        success: false,
        message: 'internal server error',
        statusCode: 500,
      }
    } else {
      return {
        success: false,
        message: 'failed to delete request',
        statusCode: error.response?.status,
      }
    }
  }
}
