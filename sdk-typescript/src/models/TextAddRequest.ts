/* istanbul ignore file */
/* tslint:disable */
 

import type { Text } from './Text';

export type TextAddRequest = {
  documents: Array<Text>;
  repository?: string | null;
};
