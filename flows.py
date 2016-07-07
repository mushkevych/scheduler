# NOTICE: do not remove flows.py, as this module is required 
# to initialized flow.conf and run unit tests
# If no flows are registered in the system - simply keep 
# an empty file with this header

from flow.core.flow_graph import FlowGraph
from flow.core.simple_actions import ShellCommandAction, IdentityAction

from constants import SIMPLE_FLOW_NAME

sa_ls = ShellCommandAction('ls -al')
sa_mkdir = ShellCommandAction('mkdir -p /tmp/trash/flows')
sa_rmdir = ShellCommandAction('rm -Rf /tmp/trash/')
sa_identity = IdentityAction()

flows = {
    SIMPLE_FLOW_NAME: FlowGraph(SIMPLE_FLOW_NAME) \
        .append(name='step_1',
                dependent_on_names=[],
                main_action=sa_rmdir, pre_actions=[sa_mkdir],
                post_actions=[sa_ls, sa_mkdir]) \
        .append(name='step_2',
                dependent_on_names=['step_1'],
                main_action=sa_rmdir, pre_actions=[sa_mkdir],
                post_actions=[sa_ls, sa_mkdir]) \
        .append(name='step_3',
                dependent_on_names=['step_2'],
                main_action=sa_rmdir, pre_actions=[sa_mkdir],
                post_actions=[sa_ls, sa_mkdir]) \
        .append(name='step_4',
                dependent_on_names=['step_2'],
                main_action=sa_rmdir, pre_actions=[sa_mkdir],
                post_actions=[sa_ls, sa_mkdir]) \
        .append(name='step_5',
                dependent_on_names=['step_3', 'step_4'],
                main_action=sa_rmdir, pre_actions=[sa_mkdir],
                post_actions=[sa_ls, sa_mkdir]),
}
