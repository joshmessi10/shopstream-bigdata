import json
import os

def test_schema_is_valid_json():
    # El archivo schema.json debería ser un JSON válido
    schema_path = os.path.join(os.path.dirname(__file__), '..', 'schema.json')
    with open(schema_path, 'r') as f:
        schema = json.load(f)
    assert isinstance(schema, dict)
    assert "event_type" in str(schema)
