interface Props {
  enabled: boolean;
  onEnabledChange: (enabled: boolean) => void;
  scheduledTime: string;
  onScheduledTimeChange: (value: string) => void;
}

/** "Schedule Job" checkbox + datetime picker, repeated on every run form. */
export function ScheduleInput({
  enabled,
  onEnabledChange,
  scheduledTime,
  onScheduledTimeChange,
}: Props) {
  return (
    <>
      <div className="flex items-center mb-4">
        <input
          type="checkbox"
          id="schedule_job"
          className="mr-2"
          checked={enabled}
          onChange={(event) => {
            onEnabledChange(event.target.checked);
          }}
        />
        <label htmlFor="schedule_job">Schedule Job</label>
      </div>
      {enabled && (
        <div id="scheduling_options" className="mb-4">
          <input
            type="datetime-local"
            id="scheduled_time"
            className="border-2 border-gray-400 p-2"
            value={scheduledTime}
            onChange={(event) => {
              onScheduledTimeChange(event.target.value);
            }}
          />
        </div>
      )}
    </>
  );
}
