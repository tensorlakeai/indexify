/* istanbul ignore file */
/* tslint:disable */
 

export type TextSplitterKind =
  | 'none'
  | 'new_line'
  | {
      /**
       * Split a document across the regex boundary
       */
      regex: {
        pattern: string;
      };
    };
