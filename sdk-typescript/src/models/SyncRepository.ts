/* istanbul ignore file */
/* tslint:disable */
/* eslint-disable */

import type { DataConnector } from './DataConnector';
import type { Extractor } from './Extractor';

export type SyncRepository = {
  data_connectors: Array<DataConnector>;
  extractors: Array<Extractor>;
  metadata: Record<string, any>;
  name: string;
};
