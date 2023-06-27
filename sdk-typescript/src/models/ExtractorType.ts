/* istanbul ignore file */
/* tslint:disable */
 

import type { IndexDistance } from './IndexDistance';
import type { TextSplitterKind } from './TextSplitterKind';

export type ExtractorType = {
  embedding: {
    distance: IndexDistance;
    model: string;
    text_splitter: TextSplitterKind;
  };
};
