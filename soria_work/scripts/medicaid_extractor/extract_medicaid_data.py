#!/usr/bin/env python3
"""
Simple script to extract Medicaid data using the Pulse API package
"""

from pulse_api_package import PulseExtractor, create_medicaid_schema, create_medicaid_prompt

def main():
    # Configuration
    API_KEY = "8gndJ2bjuApI03VJoB2YyoT7MUk5hKO8xfD6GtMH"
    FILE_PATH = "/Users/cameronspiller/Downloads/51301-2021-07-medicaid.pdf"
    OUTPUT_DIR = "./outputs"
    
    print("🚀 Starting Medicaid data extraction...")
    
    # Create the extractor
    extractor = PulseExtractor(api_key=API_KEY)
    
    # Run extraction with pre-built Medicaid configuration
    results = extractor.extract_from_file(
        file_path=FILE_PATH,
        system_prompt=create_medicaid_prompt(),
        json_schema=create_medicaid_schema(),
        output_dir=OUTPUT_DIR
    )
    
    # Check results
    if results["success"]:
        print("\n🎉 Extraction completed successfully!")
        print(f"📁 Files saved in: {OUTPUT_DIR}")
        if results["csv_success"]:
            print(f"✅ CSV ready for analysis: {results['csv_output']}")
        else:
            print("⚠️  JSON available but CSV conversion failed")
    else:
        print(f"\n❌ Extraction failed: {results.get('error', 'Unknown error')}")

if __name__ == "__main__":
    main() 