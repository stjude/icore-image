import { useCallback, useEffect, useState } from 'react';

import { cancelTask, deleteTask, getTasks, type TaskSummary } from '../api/endpoints';
import { StatusBadge } from '../components/StatusBadge';
import { formatCreatedAt, formatScheduledTime } from '../lib/formatDate';

export function TaskList() {
  const [tasks, setTasks] = useState<TaskSummary[] | null>(null);

  const refresh = useCallback(() => {
    getTasks()
      .then(({ tasks: loaded }) => {
        setTasks(loaded);
      })
      .catch((error: unknown) => {
        console.error('Error loading tasks:', error);
      });
  }, []);

  useEffect(() => {
    refresh();
  }, [refresh]);

  const handleCancel = async (taskId: number) => {
    if (!confirm('Are you sure you want to cancel this task?')) return;
    try {
      await cancelTask(taskId);
      refresh();
    } catch (error) {
      alert(`Failed to cancel task: ${error instanceof Error ? error.message : 'Unknown error'}`);
    }
  };

  const handleDelete = async (taskId: number) => {
    if (!confirm('Are you sure you want to delete this project?')) return;
    try {
      await deleteTask(taskId);
      refresh();
    } catch (error) {
      alert(`Failed to delete project: ${error instanceof Error ? error.message : 'Unknown error'}`);
    }
  };

  return (
    <>
      <div className="px-4">
        <h1 className="text-xl flex-1">Projects</h1>
      </div>
      <div className="container mx-auto px-4 py-8">
        <div className="bg-white shadow-md rounded-lg overflow-x-auto">
          <table className="w-full table-fixed divide-y divide-gray-200">
            <thead className="bg-gray-50">
              <tr>
                <th className="w-1/6 px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                  Name
                </th>
                <th
                  className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider"
                  style={{ minWidth: '200px' }}
                >
                  Type
                </th>
                <th className="w-1/8 px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                  Status
                </th>
                <th
                  className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider"
                  style={{ minWidth: '180px' }}
                >
                  Created
                </th>
                <th className="w-1/6 px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                  Scheduled
                </th>
                <th className="w-1/4 px-4 py-3 text-right text-xs font-medium text-gray-500 uppercase tracking-wider">
                  Actions
                </th>
              </tr>
            </thead>
            <tbody className="bg-white divide-y divide-gray-200">
              {tasks !== null && tasks.length === 0 && (
                <tr>
                  <td colSpan={6} className="px-4 py-4 text-center text-sm text-gray-500">
                    No projects found
                  </td>
                </tr>
              )}
              {(tasks ?? []).map((task) => (
                <tr key={task.id}>
                  <td className="px-4 py-4 text-sm text-gray-900 truncate">
                    <a
                      href={`/task_progress/?project_id=${task.id}`}
                      className="text-indigo-600 hover:text-indigo-900"
                    >
                      {task.name}
                    </a>
                  </td>
                  <td className="px-4 py-4 text-sm text-gray-500">{task.task_type_display}</td>
                  <td className="px-4 py-4">
                    <StatusBadge status={task.status} label={task.status_display} />
                  </td>
                  <td className="px-4 py-4 text-sm text-gray-500">
                    {formatCreatedAt(task.created_at)}
                  </td>
                  <td className="px-4 py-4 text-sm text-gray-500">
                    {task.scheduled_time ? formatScheduledTime(task.scheduled_time) : '-'}
                  </td>
                  <td className="px-4 py-4 text-sm font-medium">
                    <div className="flex space-x-4 justify-end">
                      <a
                        href={`/task_progress/?project_id=${task.id}`}
                        className="text-indigo-600 hover:text-indigo-900"
                      >
                        Open
                      </a>
                      {task.status === 'PENDING' || task.status === 'RUNNING' ? (
                        <button
                          onClick={() => void handleCancel(task.id)}
                          className="text-orange-600 hover:text-orange-900"
                        >
                          Cancel
                        </button>
                      ) : (
                        <button
                          onClick={() => void handleDelete(task.id)}
                          className="text-red-600 hover:text-red-900"
                        >
                          Delete
                        </button>
                      )}
                    </div>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>
    </>
  );
}
