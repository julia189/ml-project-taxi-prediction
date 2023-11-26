from sagemaker.workflows.paramter import (
    ParameterInteger,
    ParamterString,
)

processing_instance_count = ParameterInteger(
    name = 'ProcessingInstanceCount',
    default_value = 1
)

model_approval_status = ParamterString(
    name = 'ModelApprovalStatus',
    default_value = 'PendingManualApproval'
)

input_data = ParamterString(
    name = "InputData",
    default_value = input_data_uri, 
)

batch_data = ParamterString(
    name = "BatchData",
    default_value = batch_data_uri,
)