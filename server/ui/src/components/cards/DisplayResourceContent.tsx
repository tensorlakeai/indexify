import { GPUResources } from '../../types'

interface DisplayResourceContentProps {
  keyName: string
  value: string | number | GPUResources | null
}

export default function DisplayResourceContent({
  keyName,
  value,
}: DisplayResourceContentProps) {
  return (
    <p>
      <strong>{keyName}:</strong>{' '}
      {value === null
        ? 'null'
        : typeof value === 'object'
        ? JSON.stringify(value)
        : String(value)}
    </p>
  )
}
