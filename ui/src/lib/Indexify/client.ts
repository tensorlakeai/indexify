import axios, { AxiosInstance, AxiosResponse } from "axios";
import Namespace from "./namespace";
import Extractor from "./extractor";
import { IExtractor, INamespace } from "./types";

const DEFAULT_SERVICE_URL = "http://localhost:8900"; // Set your default service URL

class IndexifyClient {
  private serviceUrl: string;
  private client: AxiosInstance;

  constructor(serviceUrl: string = DEFAULT_SERVICE_URL) {
    this.serviceUrl = serviceUrl;
    this.client = axios.create();
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

  async namespaces(): Promise<Namespace[]> {
    const response = await this.get("namespaces");
    const namespacesData = response.data.namespaces as INamespace[];
    return namespacesData.map(
      (data) => new Namespace(this.serviceUrl, data.name)
    );
  }

  async getNamespace(name: string): Promise<Namespace> {
    const response = await this.get(`namespaces/${name}`);
    const data = response.data.namespace as INamespace;
    return new Namespace(this.serviceUrl, data.name, data.extractor_bindings);
  }

  async extractors(): Promise<Extractor[]> {
    const response = await this.get("extractors");
    const extractorsData = response.data.extractors as IExtractor[];
    return extractorsData.map((data) => new Extractor(data));
  }
}

export default IndexifyClient;
