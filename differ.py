import streamlit as st
from streamlit.runtime.uploaded_file_manager import UploadedFile
import json
import jsondiff
import pandas as pd
from functions.flatten import flatten_keys
from functions import tidy
from pathlib import Path
import os

# Minimal viable imports from dbt-core
from dbt.contracts.graph.manifest import WritableManifest
from dbt.graph.selector_methods import StateSelectorMethod

class MockPreviousState:
    def __init__(self, manifest: WritableManifest) -> None:
        self.manifest: Manifest = manifest

st.set_page_config(layout="wide")
st.title("dbt Manifest Differ")

st.info("""Work out why your models built in a Slim CI run.
This will look for your production manifest in $DATACOVES_DBT_HOME/logs
and you current branch manifest in $DATACOVES_DBT_HOME/target

False positives in `modified.configs` are likely to be due to `config()` blocks in a node's definition or in its `.yml` resource file.

To avoid false positives, define configs in `dbt_project.yml` instead.
See [the docs on state comparison](https://docs.getdbt.com/reference/node-selection/state-comparison-caveats#false-positives) for more information.
""", icon = "💡")

# Copy-paste from https://github.com/dbt-labs/dbt-core/blob/0ab954e1af9bb2be01fa4ebad2df7626249a1fab/core/dbt/graph/selector_methods.py#L676
state_options = [
    "modified",
    "new",
    "modified.body",
    "modified.configs",
    "modified.persisted_descriptions",
    "modified.relation",
    "modified.macros",
    "modified.contract"
]

state_method = st.selectbox(label="State comparison method:", options=state_options)
properties_to_ignore = st.multiselect("Properties to ignore when showing node-level diffs:", ['created_at', 'root_path', 'build_path', 'compiled_path', 'deferred', 'schema', 'checksum', 'compiled_code', 'database', 'relation_name'], default=['created_at', 'checksum', 'database', 'schema', 'relation_name', 'compiled_path', 'root_path', 'build_path'])
skipped_large_seeds = set()

def load_manifest(file_name: str) -> WritableManifest:
    with open(file_name, 'r') as file:
        data = json.load(file)
        data, large_seeds = tidy.remove_large_seeds(data)
        skipped_large_seeds.update(large_seeds)
        return WritableManifest.upgrade_schema_version(data)

# Find the dbt project location
dbt_project_path = os.getenv("DATACOVES__DBT_HOME")

if dbt_project_path:
    production_manifest_location = os.path.join(dbt_project_path, "logs", "manifest.json")
    branch_manifest_location = os.path.join(dbt_project_path, "target", "manifest.json")

    not_found_files = []

    if os.path.exists(production_manifest_location) and os.path.exists(branch_manifest_location):
        production_manifest = load_manifest(production_manifest_location)
        branch_manifest = load_manifest(branch_manifest_location)
    else:
        if not os.path.exists(production_manifest_location):
            not_found_files.append("Production manifest file")
        if not os.path.exists(branch_manifest_location):
            not_found_files.append("Branch manifest file")

        error_message = f"Manifest files not found: {', '.join(not_found_files)}. Please make sure the paths are correct."
        st.warning(error_message)
else:
    st.warning("DATACOVES__DBT_HOME environment variable not set.")

if not not_found_files:
    # TODO: also calculate diffs for sources, exposures, semantic_models, metrics
    included_nodes = set(branch_manifest.nodes.keys())
    previous_state = MockPreviousState(production_manifest)
    state_comparator = StateSelectorMethod(branch_manifest, previous_state, "")

    if len(skipped_large_seeds) > 0:
        st.warning(f"Some large seeds couldn't be compared from the manifest alone: {skipped_large_seeds}" )

    state_inclusion_counts = {}
    state_inclusion_reasons_by_node = {}
    for state_option in state_options:
        results = list(state_comparator.search(included_nodes, state_option))
        for node in results:
            if node in state_inclusion_reasons_by_node:
                state_inclusion_reasons_by_node[node].append(state_option)
            else:
                state_inclusion_reasons_by_node[node] = [state_option]
        state_inclusion_counts[state_option] = len((results))

    st.bar_chart(state_inclusion_counts)
    selected_nodes = list(state_comparator.search(included_nodes, state_method))

    if state_comparator.modified_macros:
        st.header("Modified macros")
        st.write(state_comparator.modified_macros)

    st.header(f"{len(selected_nodes)} Selected node{'s' if len(selected_nodes) != 1 else ''}")
    for unique_id in selected_nodes:

        left_node = branch_manifest.nodes.get(unique_id)
        right_node = production_manifest.nodes.get(unique_id)
        st.subheader(unique_id)

        if left_node and right_node:
            left_dict = left_node.to_dict()
            right_dict = right_node.to_dict()
            all_keys = set(left_dict.keys()) | set(right_dict.keys())
            diffs = {
                k: jsondiff.diff(
                    left_dict.get(k, None),
                    right_dict.get(k, None),
                    syntax='symmetric',
                    marshal=True
                )
                for k in all_keys
                if k not in properties_to_ignore and (
                    k not in right_dict
                    or k not in left_dict
                    or left_dict[k] != right_dict[k]
                )
            }

            st.write("##### State selectors that find this node:")
            st.code(state_inclusion_reasons_by_node[unique_id])

            if left_node.depends_on.macros and state_comparator.modified_macros:
                st.write(f"Depends on macros: {left_node.depends_on.macros}")

            diff_json, right_full_json = st.columns(2)

            diff_json.write("##### JSON tree of diffs:")
            diff_json.json(diffs, expanded=False)

            right_full_json.write("##### JSON tree of all elements in right node:")
            right_full_json.json(right_dict, expanded=False)

            st.write("##### Flat table of diffs:")
            try:
                flattened_diff = flatten_keys(diffs)
                df = pd.DataFrame.from_dict(flattened_diff, orient='index')
                st.dataframe(df, use_container_width=True)
            except Exception as e:
                st.error(f"Couldn't print as table: {e}")


        elif not left_node:
            st.warning(f"Missing from branch manifest (deleted node)")

        elif not right_node:
            st.warning(f"Missing from production manifest (new node)")
            st.write("State methods that pick this node up:")
            st.code(state_inclusion_reasons_by_node[unique_id])

        st.divider()

else:
    st.warning("Production and branch manifests needed to perform comparison", icon="👯")
