:- use_module(library(scasp)).

% define different predicates to do stuff

yba(restart_nodes, [rolling, wait_time(seconds(180))]).
yba(restart_nodes, [instant]).
yba(add_node).
yba(add_region).
yba(remove_node).
yba(upgrade).
yba(backup).
yba(pitr).

ybadmin(flush).
ybadmin(comapct).
ybadmin(delete_tablet).

% differnt kinds of nemesis
nemesis(shutdown, Options).
nemesis(vm_restart, Options).
nemesis(kernel_shutdown, Options).
nemesis(detach_volume, Options).
nemesis(network_slowdown, Options).
nemesis(clock_skew, Options).
nemesis(network_package_loss, Options).
nemesis(process_kill, Options).
nemesis(corrupt_files, Options).
nemesis(mount_unmount, Options).
nemesis(dns, Options).

master(Node).
tserver(Node).
master_leader(Node).

workload('SqlBankTransfer', Options).
workload('SqlBankLargeValues', Options).
workload('SqlBankMultipleTableTransfer', Options).
workload('SqlBankWaitOnConflict', Options).
workload('SqlBankTransferVerification', Options).
workload('SqlInserts', Options).
workload('SqlUpdates', Options).
workload('SqlSecondaryIndex', Options).
workload('SqlDataLoad', Options).
workload('SqlIntensiveConsistencyDDL', Options).
workload('SqlForeignKeysAndJoins', Options).
workload('SqlFKAndBatchedJoins', Options).
workload('SqlSnapshotTxns', Options).
workload('SqlDataLoadWithDDL', Options).
workload('SqlTransactions', Options).
workload('SqlSequence', Options).
workload('SqlGeoTransactions', Options).
workload('SqlGeoPartitionedTable', Options).
workload('SqlColocationWorkload', Options).
workload('SqlWaitOnConflictTransactions', Options).
workload('SqlWaitQueues', Options).
workload('SqlConnectionsBurst', Options).
workload('SqlCrossDBLoadWithDDL', Options).
workload('SqlReadCommitted', Options).


is_a_op(X) :- functor(T, Name, _), member(Name, ['nemesis', 'workload', 'ybadmin', 'yba']).
