/* istanbul ignore file */
/* tslint:disable */
/* eslint-disable */

import type { Text } from './Text';

export type TextAddRequest = {
    documents: Array<Text>;
    namespace?: string | null;
};

