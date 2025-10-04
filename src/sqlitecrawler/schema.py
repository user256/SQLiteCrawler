"""
Schema.org structured data extraction and parsing.

This module handles extraction of structured data from HTML pages including:
- JSON-LD (application/ld+json)
- Microdata (itemscope, itemtype, itemprop)
- RDFa (vocab, typeof, property)
"""

import json
import re
from typing import List, Dict, Any, Optional, Tuple
from bs4 import BeautifulSoup, Tag
from urllib.parse import urljoin


def extract_schema_data(html: str, base_url: str) -> List[Dict[str, Any]]:
    """
    Extract all structured data from HTML.
    Returns a list of schema data dictionaries.
    """
    soup = BeautifulSoup(html, 'html.parser')
    schema_data = []
    
    # Extract JSON-LD
    json_ld_data = extract_json_ld(soup, base_url)
    schema_data.extend(json_ld_data)
    
    # Extract Microdata
    microdata = extract_microdata(soup, base_url)
    schema_data.extend(microdata)
    
    # Extract RDFa
    rdfa_data = extract_rdfa(soup, base_url)
    schema_data.extend(rdfa_data)
    
    return schema_data


def extract_json_ld(soup: BeautifulSoup, base_url: str) -> List[Dict[str, Any]]:
    """Extract JSON-LD structured data from script tags."""
    schema_data = []
    
    # Find all script tags with type="application/ld+json"
    script_tags = soup.find_all('script', type='application/ld+json')
    
    for i, script in enumerate(script_tags):
        try:
            # Parse JSON content
            json_content = script.string.strip() if script.string else ""
            if not json_content:
                continue
                
            # Handle both single objects and arrays
            try:
                data = json.loads(json_content)
            except json.JSONDecodeError as e:
                schema_data.append({
                    'format': 'json-ld',
                    'type': 'InvalidJSON',
                    'raw_data': json_content,
                    'parsed_data': None,
                    'position': i,
                    'is_valid': False,
                    'validation_errors': [f"JSON decode error: {str(e)}"]
                })
                continue
            
            # Handle arrays of schema objects
            if isinstance(data, list):
                for j, item in enumerate(data):
                    schema_item = process_json_ld_item(item, json_content, i * 100 + j, base_url)
                    if schema_item:
                        schema_data.append(schema_item)
            else:
                # Single schema object
                schema_item = process_json_ld_item(data, json_content, i, base_url)
                if schema_item:
                    schema_data.append(schema_item)
                    
        except Exception as e:
            schema_data.append({
                'format': 'json-ld',
                'type': 'ParseError',
                'raw_data': str(script),
                'parsed_data': None,
                'position': i,
                'is_valid': False,
                'validation_errors': [f"Parse error: {str(e)}"]
            })
    
    return schema_data


def process_json_ld_item(data: Dict[str, Any], raw_json: str, position: int, base_url: str) -> Optional[Dict[str, Any]]:
    """Process a single JSON-LD item and extract schema type."""
    if not isinstance(data, dict):
        return None
    
    # Extract @type or determine type from context
    schema_type = data.get('@type', 'Unknown')
    
    # Clean up the type (remove schema.org prefix if present)
    if isinstance(schema_type, str):
        schema_type = schema_type.replace('https://schema.org/', '').replace('http://schema.org/', '')
    elif isinstance(schema_type, list) and schema_type:
        # Handle multiple types - use the first one
        schema_type = str(schema_type[0]).replace('https://schema.org/', '').replace('http://schema.org/', '')
    
    # Normalize and validate the data
    normalized_data = normalize_schema_data(data, base_url)
    validation_errors = validate_schema_data(normalized_data, schema_type)
    
    return {
        'format': 'json-ld',
        'type': schema_type,
        'raw_data': raw_json,
        'parsed_data': json.dumps(normalized_data) if normalized_data else None,
        'position': position,
        'is_valid': len(validation_errors) == 0,
        'validation_errors': validation_errors
    }


def extract_microdata(soup: BeautifulSoup, base_url: str) -> List[Dict[str, Any]]:
    """Extract microdata structured data."""
    schema_data = []
    
    # Find all elements with itemscope
    items = soup.find_all(attrs={'itemscope': True})
    
    for i, item in enumerate(items):
        try:
            # Extract itemtype
            itemtype = item.get('itemtype', '')
            if not itemtype:
                continue
            
            # Clean up the type
            schema_type = itemtype.replace('https://schema.org/', '').replace('http://schema.org/', '')
            
            # Extract properties
            properties = extract_microdata_properties(item, base_url)
            
            # Create normalized data structure
            normalized_data = {
                '@type': schema_type,
                **properties
            }
            
            validation_errors = validate_schema_data(normalized_data, schema_type)
            
            schema_data.append({
                'format': 'microdata',
                'type': schema_type,
                'raw_data': str(item),
                'parsed_data': json.dumps(normalized_data),
                'position': i,
                'is_valid': len(validation_errors) == 0,
                'validation_errors': validation_errors
            })
            
        except Exception as e:
            schema_data.append({
                'format': 'microdata',
                'type': 'ParseError',
                'raw_data': str(item),
                'parsed_data': None,
                'position': i,
                'is_valid': False,
                'validation_errors': [f"Parse error: {str(e)}"]
            })
    
    return schema_data


def extract_microdata_properties(item: Tag, base_url: str) -> Dict[str, Any]:
    """Extract properties from a microdata item."""
    properties = {}
    
    # Find all itemprop elements within this item
    prop_elements = item.find_all(attrs={'itemprop': True})
    
    for prop in prop_elements:
        prop_name = prop.get('itemprop', '')
        if not prop_name:
            continue
        
        # Extract the value
        if prop.name in ['img', 'audio', 'video', 'source']:
            # Media elements - get src
            value = prop.get('src', '')
        elif prop.name == 'a':
            # Links - get href
            value = prop.get('href', '')
        elif prop.name == 'meta':
            # Meta tags - get content
            value = prop.get('content', '')
        elif prop.name == 'time':
            # Time elements - get datetime or text
            value = prop.get('datetime', prop.get_text(strip=True))
        else:
            # Other elements - get text content
            value = prop.get_text(strip=True)
        
        # Convert relative URLs to absolute
        if isinstance(value, str) and value.startswith('/'):
            value = urljoin(base_url, value)
        
        # Handle multiple properties with same name
        if prop_name in properties:
            if not isinstance(properties[prop_name], list):
                properties[prop_name] = [properties[prop_name]]
            properties[prop_name].append(value)
        else:
            properties[prop_name] = value
    
    return properties


def extract_rdfa(soup: BeautifulSoup, base_url: str) -> List[Dict[str, Any]]:
    """Extract RDFa structured data."""
    schema_data = []
    
    # Find all elements with typeof
    items = soup.find_all(attrs={'typeof': True})
    
    for i, item in enumerate(items):
        try:
            # Extract typeof
            typeof = item.get('typeof', '')
            if not typeof:
                continue
            
            # Clean up the type
            schema_type = typeof.replace('https://schema.org/', '').replace('http://schema.org/', '')
            
            # Extract properties
            properties = extract_rdfa_properties(item, base_url)
            
            # Create normalized data structure
            normalized_data = {
                '@type': schema_type,
                **properties
            }
            
            validation_errors = validate_schema_data(normalized_data, schema_type)
            
            schema_data.append({
                'format': 'rdfa',
                'type': schema_type,
                'raw_data': str(item),
                'parsed_data': json.dumps(normalized_data),
                'position': i,
                'is_valid': len(validation_errors) == 0,
                'validation_errors': validation_errors
            })
            
        except Exception as e:
            schema_data.append({
                'format': 'rdfa',
                'type': 'ParseError',
                'raw_data': str(item),
                'parsed_data': None,
                'position': i,
                'is_valid': False,
                'validation_errors': [f"Parse error: {str(e)}"]
            })
    
    return schema_data


def extract_rdfa_properties(item: Tag, base_url: str) -> Dict[str, Any]:
    """Extract properties from an RDFa item."""
    properties = {}
    
    # Find all elements with property within this item
    prop_elements = item.find_all(attrs={'property': True})
    
    for prop in prop_elements:
        prop_name = prop.get('property', '')
        if not prop_name:
            continue
        
        # Clean up property name
        prop_name = prop_name.replace('https://schema.org/', '').replace('http://schema.org/', '')
        
        # Extract the value
        if prop.name in ['img', 'audio', 'video', 'source']:
            # Media elements - get src
            value = prop.get('src', '')
        elif prop.name == 'a':
            # Links - get href
            value = prop.get('href', '')
        elif prop.name == 'meta':
            # Meta tags - get content
            value = prop.get('content', '')
        elif prop.name == 'time':
            # Time elements - get datetime or text
            value = prop.get('datetime', prop.get_text(strip=True))
        else:
            # Other elements - get text content
            value = prop.get_text(strip=True)
        
        # Convert relative URLs to absolute
        if isinstance(value, str) and value.startswith('/'):
            value = urljoin(base_url, value)
        
        # Handle multiple properties with same name
        if prop_name in properties:
            if not isinstance(properties[prop_name], list):
                properties[prop_name] = [properties[prop_name]]
            properties[prop_name].append(value)
        else:
            properties[prop_name] = value
    
    return properties


def normalize_schema_data(data: Dict[str, Any], base_url: str) -> Dict[str, Any]:
    """Normalize schema data by converting relative URLs to absolute."""
    if not isinstance(data, dict):
        return data
    
    normalized = {}
    for key, value in data.items():
        if isinstance(value, str) and value.startswith('/'):
            # Convert relative URL to absolute
            normalized[key] = urljoin(base_url, value)
        elif isinstance(value, dict):
            # Recursively normalize nested objects
            normalized[key] = normalize_schema_data(value, base_url)
        elif isinstance(value, list):
            # Normalize list items
            normalized[key] = [
                normalize_schema_data(item, base_url) if isinstance(item, dict) else item
                for item in value
            ]
        else:
            normalized[key] = value
    
    return normalized


def validate_schema_data(data: Dict[str, Any], schema_type: str) -> List[str]:
    """Validate schema data and return list of validation errors."""
    errors = []
    
    if not isinstance(data, dict):
        errors.append("Schema data must be an object")
        return errors
    
    # Basic validation for common schema types
    if schema_type.lower() == 'article':
        if not data.get('headline'):
            errors.append("Article missing required 'headline' property")
        if not data.get('author'):
            errors.append("Article missing required 'author' property")
    
    elif schema_type.lower() == 'product':
        if not data.get('name'):
            errors.append("Product missing required 'name' property")
        if not data.get('offers'):
            errors.append("Product missing required 'offers' property")
    
    elif schema_type.lower() == 'organization':
        if not data.get('name'):
            errors.append("Organization missing required 'name' property")
    
    elif schema_type.lower() == 'breadcrumblist':
        if not data.get('itemListElement'):
            errors.append("BreadcrumbList missing required 'itemListElement' property")
    
    # Validate URLs
    for key, value in data.items():
        if 'url' in key.lower() and isinstance(value, str):
            if not value.startswith(('http://', 'https://', '/')):
                errors.append(f"Invalid URL format for {key}: {value}")
    
    return errors


def get_schema_statistics(schema_data: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Get statistics about extracted schema data."""
    stats = {
        'total_schemas': len(schema_data),
        'by_format': {},
        'by_type': {},
        'valid_count': 0,
        'invalid_count': 0,
        'validation_errors': []
    }
    
    for item in schema_data:
        # Count by format
        format_name = item.get('format', 'unknown')
        stats['by_format'][format_name] = stats['by_format'].get(format_name, 0) + 1
        
        # Count by type
        type_name = item.get('type', 'unknown')
        stats['by_type'][type_name] = stats['by_type'].get(type_name, 0) + 1
        
        # Count valid/invalid
        if item.get('is_valid', False):
            stats['valid_count'] += 1
        else:
            stats['invalid_count'] += 1
            errors = item.get('validation_errors', [])
            stats['validation_errors'].extend(errors)
    
    return stats
