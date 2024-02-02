import { IExtractor, IExtractorSchema } from "./types";

class Extractor {
  public name: string;
  public description: string;
  public input_params: Record<string, string | number>;
  public outputs: IExtractorSchema;

  constructor(data: IExtractor) {
    this.name = data.name;
    this.description = data.description;
    this.input_params = data.input_params;
    this.outputs = data.outputs;
  }
}

export default Extractor;
