# scripts/rename_checkpoints.py

from great_expectations.data_context import get_context
from great_expectations.checkpoint import Checkpoint

rename_map = {
    "UTP_Project_Info_CSJ_List_asset_checkpoint": "utp_project_info_csj_list_checkpoint",
    "UTP_Project_Info_Comments_&_Actions_asset_checkpoint": "utp_project_info_comments_actions_checkpoint",
    "UTP_Project_Info_Project_Names_asset_checkpoint": "utp_project_info_project_names_checkpoint",
    "UTP_Project_Info_Project_Tiers_asset_checkpoint": "utp_project_info_project_tiers_checkpoint",
    "UTP_Project_Info_Project_Group_ID_asset_checkpoint": "utp_project_info_project_group_id_checkpoint",
    "UTP_Project_Info_HWY_Various_asset_checkpoint": "utp_project_info_hwy_various_checkpoint",
}

context = get_context()

for old_name, new_name in rename_map.items():
    try:
        # Get existing checkpoint
        checkpoint = context.get_checkpoint(old_name)

        # Manually extract checkpoint config details
        config = {
            "name": new_name,
            "config_version": checkpoint.config_version,
            "class_name": checkpoint.__class__.__name__,
            "validations": checkpoint.validations,
            "batch_request": checkpoint.batch_request,
            "expectation_suite_name": checkpoint.expectation_suite_name,
            "action_list": getattr(checkpoint, "action_list", None),
            "evaluation_parameters": getattr(checkpoint, "evaluation_parameters", None),
            "runtime_configuration": getattr(checkpoint, "runtime_configuration", None),
            "profilers": getattr(checkpoint, "profilers", None),
            "template_name": getattr(checkpoint, "template_name", None),
        }

        # Clean out None values
        config = {k: v for k, v in config.items() if v is not None}

        # Add new checkpoint
        context.add_or_update_checkpoint(**config)

        # Delete old checkpoint
        context.delete_checkpoint(old_name)

        print(f"✅ Renamed {old_name} ➡️ {new_name}")

    except Exception as e:
        print(f"⚠️ Failed to rename {old_name}: {e}")

print("✅ All checkpoint renames attempted.")