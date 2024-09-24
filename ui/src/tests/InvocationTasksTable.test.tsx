import { render, screen, waitFor } from '@testing-library/react';
import axios from 'axios';
import InvocationTasksTable from '../components/tables/InvocationTasksTable';

jest.mock('axios');
const mockedAxios = axios as jest.Mocked<typeof axios>;

const mockTasks = [
  {
    "id": "a5315d219ff5512",
    "namespace": "default",
    "compute_fn": "extractor_a",
    "compute_graph": "graph_a",
    "invocation_id": "83cc4443d738b61a",
    "input_key": "83cc4443d738b61a",
    "outcome": "Success"
  },
  {
    "id": "40d9d61be1892617",
    "namespace": "default",
    "compute_fn": "extractor_b",
    "compute_graph": "graph_a",
    "invocation_id": "83cc4443d738b61a",
    "input_key": "default|graph_a|83cc4443d738b61a|extractor_a|23b534e020ba7756",
    "outcome": "Success"
  },
  {
    "id": "67acb6c81c26ae5",
    "namespace": "default",
    "compute_fn": "extractor_c",
    "compute_graph": "graph_a",
    "invocation_id": "83cc4443d738b61a",
    "input_key": "default|graph_a|83cc4443d738b61a|extractor_b|5d2dd3c089a04bdc",
    "outcome": "Success"
  },
  {
    "id": "e55136bcfdbc1450",
    "namespace": "default",
    "compute_fn": "extractor_c",
    "compute_graph": "graph_a",
    "invocation_id": "83cc4443d738b61a",
    "input_key": "default|graph_a|83cc4443d738b61a|extractor_b|e91dd8519b99e157",
    "outcome": "Success"
  }
];

describe('TasksTable', () => {
  beforeEach(() => {
    mockedAxios.get.mockReset();
  });

  it('renders tasks table with correct data', async () => {
    mockedAxios.get.mockResolvedValue({ data: mockTasks });

    render(<InvocationTasksTable invocationId="83cc4443d738b61a" namespace="default" computeGraph="graph_a" />);

    await waitFor(() => {
      expect(screen.getByText('Tasks for Invocation 83cc4443d738b61a')).toBeInTheDocument();
    });

    expect(screen.getByText('ID')).toBeInTheDocument();
    expect(screen.getByText('Compute Function')).toBeInTheDocument();
    expect(screen.getByText('Input Key')).toBeInTheDocument();
    expect(screen.getByText('Outcome')).toBeInTheDocument();

    mockTasks.forEach(task => {
      expect(screen.getByText(task.id)).toBeInTheDocument();
      expect(screen.getByText(task.compute_fn)).toBeInTheDocument();
      expect(screen.getByText(task.input_key)).toBeInTheDocument();
      expect(screen.getByText(task.outcome)).toBeInTheDocument();
    });
  });

  it('handles API error', async () => {
    mockedAxios.get.mockRejectedValue(new Error('API Error'));

    render(<InvocationTasksTable invocationId="83cc4443d738b61a" namespace="default" computeGraph="graph_a" />);

    await waitFor(() => {
      expect(screen.getByText('Tasks for Invocation 83cc4443d738b61a')).toBeInTheDocument();
    });

    expect(screen.queryByText('a5315d219ff5512')).not.toBeInTheDocument();
  });
});