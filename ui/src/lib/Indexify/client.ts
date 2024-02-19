import axios, { AxiosInstance, AxiosResponse } from "axios";
import Extractor from "./extractor";
import {
  IContent,
  IContentMetadata,
  IExtractor,
  IExtractionPolicy,
  IIndex,
  INamespace,
  ITask,
} from "./types";

const DEFAULT_SERVICE_URL = "http://localhost:8900"; // Set your default service URL

class IndexifyClient {
  private serviceUrl: string;
  private client: AxiosInstance;
  public namespace: string;
  public extractionPolicies: IExtractionPolicy[];

  constructor(
    serviceUrl: string = DEFAULT_SERVICE_URL,
    namespace: string = "default",
    extractionPolicies: IExtractionPolicy[]
  ) {
    this.serviceUrl = serviceUrl;
    this.namespace = namespace;
    this.extractionPolicies = extractionPolicies;
    this.client = axios.create({
      baseURL: `${serviceUrl}/namespaces/${namespace}`,
    });
  }

  static async createClient({
    serviceUrl = DEFAULT_SERVICE_URL,
    namespace = "default",
  }: {
    serviceUrl?: string;
    namespace?: string;
  } = {}): Promise<IndexifyClient> {
    const response = await axios.get(`${serviceUrl}/namespaces/${namespace}`);
    return new IndexifyClient(
      serviceUrl,
      namespace,
      response.data.namespace.extraction_policies
    );
  }

  private async request(
    method: string,
    endpoint: string,
    options: any = {}
  ): Promise<AxiosResponse> {
    try {
      const response = await this.client.request({
        method,
        url: `${this.serviceUrl}/${endpoint}`,
        ...options,
      });
      return response;
    } catch (error) {
      if (axios.isAxiosError(error)) {
        console.error(`Error: ${error.message}`);
      }
      throw error;
    }
  }

  async get(endpoint: string): Promise<AxiosResponse> {
    return this.request("GET", endpoint);
  }

  async post(endpoint: string): Promise<AxiosResponse> {
    return this.request("POST", endpoint);
  }

  static async namespaces(): Promise<INamespace[]> {
    const response = await axios.get(`${DEFAULT_SERVICE_URL}/namespaces`);
    return response.data.namespaces;
  }

  async indexes(): Promise<IIndex[]> {
    const resp = await this.client.get("indexes");
    return resp.data.indexes;
  }

  async extractors(): Promise<Extractor[]> {
    const response = await this.get("extractors");
    const extractorsData = response.data.extractors as IExtractor[];
    return extractorsData.map((data) => new Extractor(data));
  }

  async getContent(
    parent_id?: string,
    labels_eq?: string
  ): Promise<IContentMetadata[]> {
    const resp = await this.client.get("content", {
      params: { parent_id, labels_eq },
    });
    return resp.data.content_list;
  }

  async getContentById(id: string): Promise<IContent> {
    const resp = await this.client.get(`content/${id}`);
    return resp.data.content_list[0];
  }

  async getTasks(extraction_policy?: string): Promise<ITask[]> {
    const resp = await this.client.get("tasks", {
      params: { extraction_policy },
    });
    return resp.data.tasks;
  }
}

export default IndexifyClient;
