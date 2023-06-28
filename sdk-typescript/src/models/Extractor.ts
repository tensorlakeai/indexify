/* istanbul ignore file */
/* tslint:disable */
/* eslint-disable */

import type { ExtractorContentType } from './ExtractorContentType';
import type { ExtractorType } from './ExtractorType';

export type Extractor = {
    content_type: ExtractorContentType;
    extractor_type: ExtractorType;
    name: string;
};

