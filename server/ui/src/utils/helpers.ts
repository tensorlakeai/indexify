const BYTE_SIZES = ['Bytes', 'KB', 'MB', 'GB', 'TB', 'PB', 'EB', 'ZB', 'YB'] as const
const BYTE_MULTIPLIER = 1000
const TIMESTAMP_THRESHOLD = 1e12
const API_KEY_PATTERNS = new Set([
  'key',
  'api_key',
  'key_api',
  'api',
  'api-key'
].map(pattern => pattern.toLowerCase()))

interface DateFormatOptions extends Intl.DateTimeFormatOptions {
  year: 'numeric'
  month: 'short'
  day: '2-digit'
  hour: '2-digit'
  minute: '2-digit'
  second: '2-digit'
  hour12: true
}

// Generates a consistent color hash from a string
export function stringToColor(input: string): string {
  const hash = Array.from(input).reduce(
    (acc, char) => ((acc << 5) + acc) + char.charCodeAt(0),
    5381
  )
  return `#${(hash & 0xFFFFFF).toString(16).padStart(6, '0')}`
}

// Returns the appropriate service URL based on environment
export function getIndexifyServiceURL(): string {
  return process.env.NODE_ENV === 'development' 
    ? 'http://localhost:8900' 
    : window.location.origin
}

// Formats byte sizes into human-readable strings
export function formatBytes(bytes: number, decimals = 2): string {
  if (bytes === 0) return '0 Bytes'
  
  const exponent = Math.floor(Math.log(bytes) / Math.log(BYTE_MULTIPLIER))
  const value = bytes / Math.pow(BYTE_MULTIPLIER, exponent)
  
  return `${value.toFixed(decimals)} ${BYTE_SIZES[exponent]}`
}

// Converts an object of key-value pairs into an array of formatted strings
export function splitLabels(data: Record<string, string>): string[] {
  return Object.entries(data).map(([key, value]) => `${key}: ${value}`)
}

const DATE_FORMAT_OPTIONS: DateFormatOptions = {
  year: 'numeric',
  month: 'short',
  day: '2-digit',
  hour: '2-digit',
  minute: '2-digit',
  second: '2-digit',
  hour12: true
}

// Formats timestamps into localized date strings
export function formatTimestamp(value: string | number | null | undefined): string {
  if (value == null) return 'N/A'
  
  const timestamp = typeof value === 'string' ? parseInt(value, 10) : value
  
  if (typeof timestamp !== 'number' || isNaN(timestamp)) return 'Invalid Date'
  
  const milliseconds = timestamp < TIMESTAMP_THRESHOLD 
    ? timestamp * 1000 
    : timestamp
  
  return new Date(milliseconds).toLocaleString(undefined, DATE_FORMAT_OPTIONS)
}

// Masks sensitive API keys in JSON strings
export function maskApiKeys(input: string): string {
  try {
    const data = JSON.parse(input) as Record<string, unknown>
    
    return JSON.stringify(
      Object.fromEntries(
        Object.entries(data).map(([key, value]) => [
          key,
          API_KEY_PATTERNS.has(key.toLowerCase())
            ? '*'.repeat(String(value).length)
            : value
        ])
      )
    )
  } catch {
    return input.replace(
      /"(key|api_key|key_api|api|api-key)":\s*"([^"]*)"/gi,
      (_, key, value) => `"${key}":"${'*'.repeat(value.length)}"`
    )
  }
}
