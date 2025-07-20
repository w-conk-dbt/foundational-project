{% macro generate_gsheet_exposures_from_api() %}
    {% if execute %}
        {# Import required Python libraries #}
        {% set google_auth = modules.google.oauth2.credentials %}
        {% set discovery = modules.googleapiclient.discovery %}
        
        {# Get credentials from dbt project variables #}
        {% set credentials_info = var('google_sheets_credentials') %}
        
        {# Initialize Google Sheets API client #}
        {% set credentials = google_auth.Credentials.from_authorized_user_info(credentials_info) %}
        {% set service = discovery.build('sheets', 'v4', credentials=credentials) %}
        {% set drive_service = discovery.build('drive', 'v3', credentials=credentials) %}

        {# Get all sheets from specified folder or entire Drive #}
        {% set folder_id = var('google_sheets_folder_id', none) %}
        {% if folder_id %}
            {% set query = "mimeType='application/vnd.google-apps.spreadsheet' and '" + folder_id + "' in parents" %}
        {% else %}
            {% set query = "mimeType='application/vnd.google-apps.spreadsheet'" %}
        {% endif %}

        {% set files = drive_service.files().list(
            q=query,
            fields="files(id, name, description, owners, lastModifyingUser)"
        ).execute() %}

        {# Generate exposures for each sheet #}
        {% for file in files.get('files', []) %}
            {# Get sheet metadata including tabs #}
            {% set sheet_metadata = service.spreadsheets().get(
                spreadsheetId=file.id
            ).execute() %}

            {# Get sheet permissions to identify owners/editors #}
            {% set permissions = drive_service.permissions().list(
                fileId=file.id,
                fields="permissions(emailAddress,role)"
            ).execute() %}

            {# Find the owner from permissions #}
            {% set owner = namespace(name=none, email=none) %}
            {% for perm in permissions.get('permissions', []) %}
                {% if perm.role == 'owner' %}
                    {% set owner.email = perm.emailAddress %}
                    {# Try to get name from email pattern #}
                    {% set owner.name = owner.email.split('@')[0] | replace('.', ' ') | title %}
                {% endif %}
            {% endfor %}

            exposure:
              name: {{ file.name | trim | replace(' ', '_') | lower }}
              label: {{ file.name }}
              type: analysis

              owner:
                name: {{ owner.name }}
                email: {{ owner.email }}

              url: "https://docs.google.com/spreadsheets/d/{{ file.id }}"
              
              description: |-
                {{ file.description | default('') }}
                
                Sheets:
                {% for sheet in sheet_metadata.get('sheets', []) %}
                - {{ sheet.properties.title }}
                {% endfor %}
                
                Last modified by: {{ file.lastModifyingUser.displayName }}

              {# Parse dependencies from sheet description #}
              {% set depends_on = [] %}
              {% set description = file.description | default('') %}
              
              {# Check for models #}
              {% if description is string and description.find('dbt_models:') >= 0 %}
                  {% set model_text = description.split('dbt_models:')[1].split('\n')[0] %}
                  {% for model in model_text.split(',') %}
                      {% do depends_on.append('ref(\'' ~ model | trim ~ '\')') %}
                  {% endfor %}
              {% endif %}

              {# Check for metrics #}
              {% if description is string and description.find('metrics:') >= 0 %}
                  {% set metric_text = description.split('metrics:')[1].split('\n')[0] %}
                  {% for metric in metric_text.split(',') %}
                      {% do depends_on.append('metric(\'' ~ metric | trim ~ '\')') %}
                  {% endfor %}
              {% endif %}

              {# Output dependencies if any exist #}
              {% if depends_on | length > 0 %}
              depends_on:
                {% for dependency in depends_on %}
                - {{ dependency }}
                {% endfor %}
              {% endif %}

              meta:
                sheet_id: {{ file.id }}
                last_modified: {{ file.modifiedTime }}
                tabs: {{ sheet_metadata.get('sheets', []) | map(attribute='properties.title') | join(', ') }}

              tags: ['google_sheets']

        {% endfor %}
    {% endif %}
{% endmacro %} 