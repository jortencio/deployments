# Custom Deployment Policy
#
# @summary Custom Deployment Policy
#
# @summary This deployment policy will deploy the target control repository commit to
#          target nodes in batches.
# @param noop
#     Indicates if a noop run wil occur before enforcement.
# @param post_noop
#     If noop is set to true, set this parameter to true to enforce changes after deployment (by batch)
# @param batch_delay
#     The delay in seconds between each batch.
# @param fail_if_no_nodes
#     Toggles between failing or silently succeeding when the target environment group has no nodes.
# @param fact
#     The fact to group/batch nodes with.  Assumes this fact exists for all nodes in the target environment
plan deployments::custom_deployment (
  Boolean $noop = false,
  Boolean $post_noop = false,
  Integer $batch_delay = 60,
  Boolean $fail_if_no_nodes = true,
  String  $fact = '',
) {
  $sha = system::env('COMMIT')
  $target_node_group_id = system::env('NODE_GROUP_ID')
  $target_branch = system::env('REPO_TARGET_BRANCH')

  # Get information about the target node group
  $node_group_hash = cd4pe_deployments::get_node_group($target_node_group_id)
  if ($node_group_hash[error]) {
    fail_plan("Unable to retrieve node group ${target_node_group_id}. Error: ${node_group_hash[error]}")
  }

  # Fail if we didn't get anything back or if we got an error
  if ($node_group_hash =~ Undef) {
    fail_plan("Could not find node group with ID: ${target_node_group_id}")
  } elsif ($node_group_hash[error]) {
    fail_plan("Could not retrieve target node group id: ${target_node_group_id}. Error: ${node_group_hash[error]}")
  }

  if ($node_group_hash[result] =~ Undef) {
    fail_plan("Node group with ID ${target_node_group_id} returned no data")
  } elsif ($node_group_hash[result][environment] =~ Undef) {
    fail_plan("Could not determine the environment for node group ${target_node_group_id}. No environmnent returned")
  }

  $target_environment = $node_group_hash[result][environment]

  # If the target environment requires approval, wait for that to take place
  cd4pe_deployments::wait_for_approval($target_environment) |String $url| {}

  # Warn or fail if there are no nodes in the target environmnent
  if ($node_group_hash[result][nodes] =~ Undef) {
    $msg = "No nodes found in target node group ${node_group_hash[result][name]}"
    if ($fail_if_no_nodes) {
      fail_plan("${msg}. Set fail_if_no_nodes parameter to false to prevent this deployment failure in the future")
    } else {
      $update_target_branch_result = cd4pe_deployments::update_git_branch_ref('CONTROL_REPO', $target_branch, $sha)
      if ($update_target_branch_result[error]) {
        fail_plan("Unable to update the target branch ${target_branch} to SHA ${sha}")
      }

      $code_result = cd4pe_deployments::deploy_code($target_environment)
      $validate_code_deploy_result = cd4pe_deployments::validate_code_deploy_status($code_result)
      unless ($validate_code_deploy_result[error] =~ Undef) {
        fail_plan("Code deployment failed to target environment ${target_environment}: ${validate_code_deploy_result[error][message]}")
      }

      return "${msg}. Deploying directly to target environment and ending deployment."
    }
  }

  # Create an array of nodes relevant to the deployment
  $target_node_list = $node_group_hash[result][nodes].map | $item | {
    "'${item}'"
  }

  # Get all the fact values for a given fact for the given set of nodes
  $get_fact_values = puppetdb_query("inventory[facts.${fact}] { certname in ${target_node_list} group by facts.${fact} }")

  $fact_values = $get_fact_values.map | $item | {
    $item["facts.${fact}"]
  }

  # Get the fact value for each node
  $get_node_fact_values = puppetdb_query(
    "inventory[certname,facts.${fact}] { 
      certname in ${target_node_list} 
      group by certname, facts.${fact}
    }"
  )

  # Create a hash containing each fact value as the key and gather the nodes with matching fact values
  $fact_value_groups = $fact_values.reduce({}) | $memo, $fact_value | {
    $filter_nodes_facts = $get_node_fact_values.filter | $node_value | {
      $node_value["facts.${fact}"] == $fact_value
    }

    $get_nodes_array = $filter_nodes_facts.map | $item | {
      $item['certname']
    }

    $fact_value_nodes = {
      $fact_value => $get_nodes_array,
    }

    $memo + $fact_value_nodes
  }

  $fact_value_groups.each | $fact_value, $nodes | {
    # create a branch for the group (Though inefficient, this way we have some visibility in the puppet runs regarding the groups)
    # Ensure our prefix is always lowercase to match to puppets suggested env regex pattern
    $branch = "rolling_deployment_${fact_value}_${system::env('DEPLOYMENT_ID')}"
    $tmp_git_branch_result = cd4pe_deployments::create_git_branch('CONTROL_REPO', $branch,  $sha, true)
    if ($tmp_git_branch_result[error]) {
      fail_plan("Could not create temporary git branch ${branch}: ${tmp_git_branch_result[error]}")
    }

    $code_result = cd4pe_deployments::deploy_code($branch, $target_environment)
    $validate_code_deploy_result = cd4pe_deployments::validate_code_deploy_status($code_result)
    unless ($validate_code_deploy_result[error] =~ Undef) {
      # TODO: Clean up git branch
      $msg = "Code deployment failed to target environment ${target_environment}: ${validate_code_deploy_result[error][message]}"
      fail_plan($msg)
    }

    # Create a temporary environment node group to pin nodes to in order to run the puppet agent on
    # nodes in the target environment in batches
    $child_group = cd4pe_deployments::create_temp_node_group($target_node_group_id, $branch, true)
    if $child_group[error] {
      #TODO: Cleanup git branch
      fail_plan("Could not create temporary node group: ${child_group[error]}")
    }

    # Pin the nodes to the created environment for the given fact value
    cd4pe_deployments::pin_nodes_to_env($nodes, $child_group[result][id])

    # Run Puppet against the nodes in the batch
    $puppet_run_result = cd4pe_deployments::run_puppet($nodes, $noop, $branch)
    if $puppet_run_result[error] {
      #TODO: Cleanup temporary git branch and node group
      fail_plan("Could not orchestrate puppet agent runs: ${puppet_run_result[error]}")
    }

    # If there were failed nodes, report failures and exit
    unless ($puppet_run_result[result][nodeStates][failedNodes] =~ Undef ) {
      #Before we fail, we should try to clean up the git branch
      $delete_tmp_git_branch_failed_deploy = cd4pe_deployments::delete_git_branch('CONTROL_REPO', $child_group[result][environment])
      $delete_tmp_node_group_result = cd4pe_deployments::delete_node_group($child_group[result][id])

      $msg = "Deployment failed for ${target_branch}. ${$puppet_run_result[result][nodeStates][failedNodes]} nodes failed."
      if ($delete_tmp_git_branch_failed_deploy[error] or $delete_tmp_node_group_result[error]) {
        fail_plan("${msg}. Also unable to delete the tmporary git branch and/or node group ${child_group[result][environment]}")
      }
      else {
        fail_plan($msg)
      }
    } else {
      # Clean up the temporary temporary node group
      $delete_tmp_node_group_result = cd4pe_deployments::delete_node_group($child_group[result][id])
      if ($delete_tmp_node_group_result[error]) {
        fail_plan("Unable to delete the temporary node group ${child_group[result][name]}.")
      }

      # Clean up the temporary temporary git branch
      $delete_tmp_git_branch = cd4pe_deployments::delete_git_branch('CONTROL_REPO', $child_group[result][environment])
      if ($delete_tmp_git_branch[error]) {
        fail_plan("Unable to delete the tmporary git branch ${child_group[result][environment]}")
      }
    }

    # Sleep for the specified wait time between batches
    ctrl::sleep($batch_delay)
  }

  # Only run the following if all fact groups ran puppet successfully
  $update_target_branch_result = cd4pe_deployments::update_git_branch_ref('CONTROL_REPO', $target_branch, $sha)
  if ($update_target_branch_result[error]) {
    fail_plan("Unable to update the target branch ${target_branch} to SHA ${sha}")
  }

  $final_code_deploy_result = cd4pe_deployments::deploy_code($target_environment)
  $validate_final_code_deploy_result = cd4pe_deployments::validate_code_deploy_status($final_code_deploy_result)
  unless ($validate_final_code_deploy_result[error] =~ Undef) {
    fail_plan("Code deployment failed to target environment ${target_environment}: ${validate_final_code_deploy_result[error][message]}")
  }

  # Do a phased puppet run on release based on batches after a successful noop release
  if ($post_noop and $noop) {
    $failed_result = $fact_value_groups.reduce([]) | $memo, $fact_value_group | {
      $puppet_run_result = cd4pe_deployments::run_puppet($fact_value_group[1], false)
      if $puppet_run_result['error'] =~ NotUndef {
        $memo + [$fact_value_group[0]]
      } else {
        $memo
      }
    }

    if ($failed_result.size > 0) {
      fail_plan("Puppet run failed for the following fact values: ${failed_result}")
    }
  }
}
