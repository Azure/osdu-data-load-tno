#!/usr/bin/env python3

#  Copyright © Microsoft Corporation
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.

import sys
import argparse
import logging
import os
import json

# Import the existing csv_to_json module
from csv_to_json import create_manifest_from_csv

def main():
    parser = argparse.ArgumentParser(description='Generate OSDU manifests from CSV data using templates')
    parser.add_argument('input_csv', help='Path to input CSV file')
    parser.add_argument('template_json', help='Path to JSON template file')
    parser.add_argument('output_path', help='Output directory for generated manifests')
    parser.add_argument('--schema-path', help='Path to schema directory for validation')
    parser.add_argument('--schema-ns-name', default='<namespace>', help='Schema namespace name to replace')
    parser.add_argument('--schema-ns-value', help='Schema namespace value replacement')
    parser.add_argument('--required-template', help='Required template JSON string')
    parser.add_argument('--required-template-file', help='Path to file containing required template JSON')
    parser.add_argument('--array-parent', help='Array parent path for manifest wrapping')
    parser.add_argument('--object-parent', help='Object parent path for manifest wrapping')
    parser.add_argument('--group-filename', help='Group filename for combined output')
    parser.add_argument('--acl-viewer', help='ACL viewer to set in manifests')
    parser.add_argument('--acl-owner', help='ACL owner to set in manifests')
    parser.add_argument('--legal-tag', help='Legal tag to set in manifests')
    
    args = parser.parse_args()
    
    # Setup logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    
    try:
        # Validate input files exist
        if not os.path.exists(args.input_csv):
            logging.error(f"Input CSV file not found: {args.input_csv}")
            return 1
            
        if not os.path.exists(args.template_json):
            logging.error(f"Template JSON file not found: {args.template_json}")
            return 1
        
        # Ensure output directory exists
        try:
            os.makedirs(args.output_path, exist_ok=True)
        except Exception as e:
            logging.error(f"Failed to create output directory {args.output_path}: {e}")
            return 1
        
        # Handle required template
        required_template = args.required_template
        if args.required_template_file and os.path.exists(args.required_template_file):
            try:
                with open(args.required_template_file, 'r') as f:
                    required_template = f.read()
                logging.info(f"Loaded required template from file: {args.required_template_file}")
            except Exception as e:
                logging.error(f"Failed to read required template file {args.required_template_file}: {e}")
                return 1
        
        logging.info(f"Processing CSV: {args.input_csv}")
        logging.info(f"Using template: {args.template_json}")
        logging.info(f"Output directory: {args.output_path}")
        
        # Call the existing function
        try:
            create_manifest_from_csv(
                input_csv=args.input_csv,
                template_json=args.template_json,
                output_path=args.output_path,
                schema_path=args.schema_path,
                schema_ns_name=args.schema_ns_name,
                schema_ns_value=args.schema_ns_value,
                required_template=required_template,
                array_parent=args.array_parent,
                object_parent=args.object_parent,
                group_filename=args.group_filename,
                acl_viewer=args.acl_viewer,
                acl_owner=args.acl_owner,
                legal_tag=args.legal_tag
            )
            
            logging.info("Manifest generation completed successfully")
            return 0
            
        except FileNotFoundError as e:
            logging.error(f"File not found error: {e}")
            return 1
        except json.JSONDecodeError as e:
            logging.error(f"JSON parsing error: {e}")
            return 1
        except Exception as e:
            logging.error(f"Error during manifest generation: {e}")
            logging.exception("Full traceback:")
            return 1
        
    except Exception as e:
        logging.error(f"Unexpected error: {e}")
        logging.exception("Full traceback:")
        return 1

if __name__ == '__main__':
    sys.exit(main())
