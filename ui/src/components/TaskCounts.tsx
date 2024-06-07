import { Box, useTheme } from '@mui/material'
import { ITask, TaskStatus } from 'getindexify'
import HourglassBottomIcon from '@mui/icons-material/HourglassBottom'
import CheckCircleIcon from '@mui/icons-material/CheckCircle'
import ReportIcon from '@mui/icons-material/Report'

const countTasks = (
  tasks: ITask[]
): { unknown: number; success: number; failure: number } => {
  let unknown = 0
  let success = 0
  let failure = 0
  for (let i = 0; i < tasks.length; i++) {
    const task = tasks[i]
    if (task.outcome === TaskStatus.Unknown) {
      unknown += 1
    } else if (task.outcome === TaskStatus.Failure) {
      failure += 1
    } else if (task.outcome === TaskStatus.Success) {
      success += 1
    }
  }
  return { unknown, failure, success }
}

const TaskCounts = ({
  tasks,
  size = 'small',
}: {
  tasks: ITask[]
  size?: 'small' | 'large'
}) => {
  const theme = useTheme()
  const taskCounts = countTasks(tasks)
  const iconWidth = size === 'small' ? 15 : 25
  return (
    <Box display="flex" gap={1}>
      <Box
        display="flex"
        alignItems="center"
        sx={{ color: theme.palette.common.black }}
      >
        <HourglassBottomIcon sx={{ width: iconWidth, gap: 0.5 }} />{' '}
        {taskCounts.unknown}
      </Box>
      <Box
        display="flex"
        alignItems="center"
        sx={{ color: theme.palette.success.main, gap: 0.5 }}
      >
        <CheckCircleIcon sx={{ width: iconWidth }} />
        {taskCounts.success}
      </Box>
      <Box
        display="flex"
        alignItems="center"
        sx={{ color: theme.palette.error.main, gap: 0.5 }}
      >
        <ReportIcon sx={{ width: iconWidth }} /> {taskCounts.failure}
      </Box>
    </Box>
  )
}

export default TaskCounts
