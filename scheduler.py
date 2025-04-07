from collections import deque, defaultdict
from typing import List
import time
from dataclasses import dataclass

N_ROWS_PER_PARTITION = 1000
TOTAL_MEMORY_CAPACITY = 10000  # bytes
MAX_SLOTS_SOURCE = 1
MAX_SLOTS_MAPPER = 2
MAX_SLOTS_REDUCER = 1


TIME_PER_ROW_SOURCE = 0.002  # seconds per row
TIME_PER_ROW_MAPPER = 0.005  # seconds per row
TIME_PER_KEY_REDUCER = 0.001  # seconds per key

N_KEYS = 3  # 'a', 'b', 'c'
SIZE_PER_ROW = 8  # bytes per row
SIZE_PER_KEY = 8  # bytes per key

EST_OUTPUT_SIZE_SOURCE = N_ROWS_PER_PARTITION * SIZE_PER_ROW
EST_INPUT_SIZE_MAPPER = EST_OUTPUT_SIZE_SOURCE
EST_OUTPUT_SIZE_MAPPER = N_KEYS * SIZE_PER_KEY
EST_INPUT_SIZE_REDUCER = EST_OUTPUT_SIZE_MAPPER
EST_OUTPUT_SIZE_REDUCER = N_KEYS * SIZE_PER_KEY


@dataclass
class Message:
    key: str
    value: int
    source_op: "Operator" = None  # Where this item came from
    source_idx: int = -1  # Where in the source's output buffer


class Operator:
    def __init__(
        self,
        name,
        est_task_duration,
        est_input_size,
        est_output_size,
        max_slots,
        downstream=None,
        keys=None,
        operator_type=None,
    ):
        self.name = name
        self.est_task_duration = est_task_duration
        self.input_size = est_input_size
        self.output_size = est_output_size
        self.max_slots = max_slots
        self.current_slots = 0
        self.input_queue = deque()  # Queue of (key, value) pairs
        self.output_buffer = deque()  # Output buffer
        self.downstream = downstream or []

        # reducer-specific
        self.keys = keys or set()  # Key this operator handles
        self.state = defaultdict(int)  # Running sums
        self.operator_type = (
            operator_type  # 'source', 'mapper', 'reducer', 'global_reducer'
        )
        self.processed_partitions = 0

        if operator_type == "source":
            self.remaining_partitions = list(range(1, 11))  # 10 partitions
            self.next_mapper_idx = 0

    def has_input_data(self):
        if self.operator_type == "source":
            return bool(self.remaining_partitions)
        return bool(self.input_queue)

    def has_available_resources(self):
        return self.current_slots < self.max_slots

    def has_output_buffer_space(self):
        return True

    def buffered_outputs_size(self):
        return sum([1 for item in self.output_buffer if item is not None]) * (
            self.output_size or 1
        )

    def available_execution_slots(self):
        return self.max_slots - self.current_slots

    def estimated_task_duration(self):
        return self.est_task_duration

    def estimated_input_size(self):
        return self.input_size

    def estimated_output_size(self):
        return self.output_size

    def launch_task(self):
        """Launch a processing task for this operator."""
        self.current_slots += 1
        try:
            # SOURCE
            if self.operator_type == "source" and self.remaining_partitions:
                _ = self.remaining_partitions.pop(0)
                time.sleep(self.est_task_duration * 0.1)

                # Simple, round-robin distribution to mappers
                output = []
                for i in range(N_ROWS_PER_PARTITION):
                    key = ["a", "b", "c"][i % 3]
                    msg = Message(
                        key=key, value=1, source_op=self
                    )  # helps track each item through the pipeline
                    output.append(msg)
                    self.output_buffer.append(msg)
                    msg.source_idx = len(self.output_buffer) - 1

                target_mapper = self.downstream[self.next_mapper_idx]
                target_mapper.input_queue.extend(output)
                self.next_mapper_idx = (self.next_mapper_idx + 1) % len(self.downstream)
                self.processed_partitions += 1

            # MAPPER
            elif self.operator_type == "mapper" and self.input_queue:
                batch = []
                batch_size = min(100, len(self.input_queue))
                source_ops_to_clean = set()

                for _ in range(batch_size):
                    if self.input_queue:
                        msg = self.input_queue.popleft()
                        batch.append(msg)

                        if msg.source_op:
                            source_ops_to_clean.add(msg.source_op)

                time.sleep(self.est_task_duration * 0.1)

                # Clean up upstream op output buffers
                for source_op in source_ops_to_clean:
                    source_output_size = len(source_op.output_buffer)
                    for msg in batch:
                        if (
                            msg.source_op == source_op
                            and 0 <= msg.source_idx < source_output_size
                        ):
                            source_op.output_buffer[msg.source_idx] = None

                # Compute partial sums by key
                partial_sums = defaultdict(int)
                for msg in batch:
                    partial_sums[msg.key] += msg.value

                # Send partial sums to reducers
                for key, sum_value in partial_sums.items():
                    for reducer in self.downstream:
                        if key in reducer.keys:
                            msg = Message(key=key, value=sum_value, source_op=self)
                            self.output_buffer.append(msg)
                            msg.source_idx = len(self.output_buffer) - 1
                            reducer.input_queue.append(msg)

                num_processed = batch_size

            # REDUCER
            elif (
                self.operator_type in ["reducer", "global_reducer"]
            ) and self.input_queue:
                batch = []
                batch_size = min(100, len(self.input_queue))
                source_ops_to_clean = set()

                for _ in range(batch_size):
                    if self.input_queue:
                        msg = self.input_queue.popleft()
                        batch.append(msg)
                        if msg.source_op:
                            source_ops_to_clean.add(msg.source_op)

                # print(f"{self.name} processing batch of {len(batch)} items")
                time.sleep(self.est_task_duration * 0.1)

                for source_op in source_ops_to_clean:
                    source_output_size = len(source_op.output_buffer)
                    for msg in batch:
                        if (
                            msg.source_op == source_op
                            and 0 <= msg.source_idx < source_output_size
                        ):
                            source_op.output_buffer[msg.source_idx] = None

                # Update internal state with new values
                for msg in batch:
                    if msg.key in self.keys:
                        self.state[msg.key] += msg.value

                # Send current state to downstream operators
                for op in self.downstream:
                    for key in self.keys:
                        if key in op.keys:
                            msg = Message(
                                key=key, value=self.state[key], source_op=self
                            )
                            self.output_buffer.append(msg)
                            msg.source_idx = len(self.output_buffer) - 1
                            op.input_queue.append(msg)

                num_processed = batch_size

        finally:
            self.current_slots -= 1


class AdaptiveScheduler:
    def __init__(
        self, operators: List[Operator], source: Operator, total_memory_capacity: int
    ):
        self.operators = operators
        self.source = source
        self.budget = total_memory_capacity
        self.total_memory_capacity = total_memory_capacity
        self.last_update_time = time.time()
        self.simulation_start_time = time.time()
        self.throughput_history = []
        self.memory_usage_history = []

        self.processed_partitions = 0
        self.task_launches = {op.name: 0 for op in operators}
        self.budget_history = []

    def update_runtime_estimates(self):
        """
        Should update resource utilization and run-time estimates.
        """
        pass

    def update_budget(self):
        """
        The budget is an optimistic estimate of the memory available for new data partitions to enter the system.
        At every second, we update the memory budget using this function.
        """
        P = 0  # Total processing time per partition
        alpha = 1  # Input:Output ratio for op_i

        print("\nBudget Calculation Details:")
        print(f"Current budget: {self.budget:.2f}")

        for op in self.operators:  #  for i <- 1 to n do
            if op.operator_type == "source":
                continue

            E_i = op.available_execution_slots()
            T_i = op.estimated_task_duration()

            I_i = op.estimated_input_size()
            O_i = op.estimated_output_size()

            P_i = (T_i / E_i) * alpha

            print(
                f"  {op.name}: (T_i)={T_i:.6f}, (E_i)={E_i}, (O_i)={alpha:.4f}, (P_i)={P_i:.6f}, output_buffer_size={op.buffered_outputs_size():.2f}"
            )

            alpha = alpha * (O_i / I_i)
            P += P_i

        print(f"  Total P: {P:.6f}")

        source_output_size = self.source.output_size
        old_budget = self.budget

        budget_increment = (source_output_size / P) if P > 0 else 0
        self.budget += budget_increment
        print(
            f"  Budget increment: {source_output_size}/{P:.6f} = {budget_increment:.2f}"
        )

        self.budget_history.append(
            (time.time() - self.simulation_start_time, self.budget)
        )

        print(f"Budget updated: {old_budget:.2f} → {self.budget:.2f}")

    def run(self):
        all_done = False
        while not all_done:
            current_time = time.time()

            if current_time - self.last_update_time >= 1:
                self.update_runtime_estimates()  # Not implemented
                self.update_budget()
                self.last_update_time = current_time

                elapsed_time = current_time - self.simulation_start_time
                partitions_processed = self.source.processed_partitions
                if elapsed_time > 0:
                    throughput = (
                        partitions_processed * N_ROWS_PER_PARTITION / elapsed_time
                    )
                    self.throughput_history.append((elapsed_time, throughput))
                    print(
                        f"Throughput: {throughput:.2f} rows/sec, Completed: {partitions_processed}/10 partitions"
                    )

            # Launch task of source
            if (
                self.source.has_input_data()
                and self.source.has_available_resources()
                and self.budget >= self.source.output_size
            ):

                self.source.launch_task()
                self.budget -= self.source.output_size
                self.task_launches[self.source.name] += 1
                print(f"Source task launched. Budget reduced to {self.budget:.2f}")

            # Create set of qualified operators
            qualified_operators = set()
            for op in self.operators:
                if (
                    op.operator_type != "source"
                    and op.has_input_data()
                    and op.has_available_resources()
                    and op.has_output_buffer_space()
                ):
                    qualified_operators.add(op)

            # Luanch task
            if qualified_operators:
                selected = min(
                    qualified_operators, key=lambda op: op.buffered_outputs_size()
                )
                selected.launch_task()
                self.task_launches[selected.name] += 1

            #
            all_done = not self.source.remaining_partitions and all(
                len(op.input_queue) == 0 and op.current_slots == 0
                for op in self.operators
            )

            time.sleep(0.05)


def create_hierarchical_operators():
    all_keys = {"a", "b", "c"}

    # Create global reducers
    global_reducer_a = Operator(
        "global_reducer_a",
        N_KEYS * TIME_PER_KEY_REDUCER,
        EST_INPUT_SIZE_REDUCER,
        EST_OUTPUT_SIZE_REDUCER,
        MAX_SLOTS_REDUCER,
        keys={"a"},
        operator_type="global_reducer",
    )

    global_reducer_b = Operator(
        "global_reducer_b",
        N_KEYS * TIME_PER_KEY_REDUCER,
        EST_INPUT_SIZE_REDUCER,
        EST_OUTPUT_SIZE_REDUCER,
        MAX_SLOTS_REDUCER,
        keys={"b"},
        operator_type="global_reducer",
    )

    global_reducer_c = Operator(
        "global_reducer_c",
        N_KEYS * TIME_PER_KEY_REDUCER,
        EST_INPUT_SIZE_REDUCER,
        EST_OUTPUT_SIZE_REDUCER,
        MAX_SLOTS_REDUCER,
        keys={"c"},
        operator_type="global_reducer",
    )

    # Reducer for key 'a'
    reducer_a = Operator(
        "reducer_a",
        N_KEYS * TIME_PER_KEY_REDUCER,
        EST_INPUT_SIZE_REDUCER,
        EST_OUTPUT_SIZE_REDUCER,
        MAX_SLOTS_REDUCER,
        downstream=[global_reducer_a],
        keys={"a"},
        operator_type="reducer",
    )

    # Reducer for key 'b'
    reducer_b = Operator(
        "reducer_b",
        N_KEYS * TIME_PER_KEY_REDUCER,
        EST_INPUT_SIZE_REDUCER,
        EST_OUTPUT_SIZE_REDUCER,
        MAX_SLOTS_REDUCER,
        downstream=[global_reducer_b],
        keys={"b"},
        operator_type="reducer",
    )

    # Reducer for key 'c'
    reducer_c = Operator(
        "reducer_c",
        N_KEYS * TIME_PER_KEY_REDUCER,
        EST_INPUT_SIZE_REDUCER,
        EST_OUTPUT_SIZE_REDUCER,
        MAX_SLOTS_REDUCER,
        downstream=[global_reducer_c],
        keys={"c"},
        operator_type="reducer",
    )

    # Mappers for local reducers
    mappers_region1 = [
        Operator(
            f"mapper_r1_{i}",
            N_ROWS_PER_PARTITION * TIME_PER_ROW_MAPPER,
            EST_INPUT_SIZE_MAPPER,
            EST_OUTPUT_SIZE_MAPPER,
            MAX_SLOTS_MAPPER,
            downstream=[reducer_a, reducer_b, reducer_c],
            operator_type="mapper",
        )
        for i in range(1, 4)
    ]

    # Mappers for global reducers
    mappers_region3 = [
        Operator(
            f"mapper_r3_{i}",
            N_ROWS_PER_PARTITION * TIME_PER_ROW_MAPPER,
            EST_INPUT_SIZE_MAPPER,
            EST_OUTPUT_SIZE_MAPPER,
            MAX_SLOTS_MAPPER,
            downstream=[global_reducer_a, global_reducer_b, global_reducer_c],
            operator_type="mapper",
        )
        for i in range(1, 4)
    ]

    # Create source that feeds mappers
    source = Operator(
        "source",
        N_ROWS_PER_PARTITION * TIME_PER_ROW_SOURCE,
        None,
        EST_OUTPUT_SIZE_SOURCE,
        MAX_SLOTS_SOURCE,
        downstream=mappers_region1 + mappers_region3,
        operator_type="source",
    )

    # Collect all operators
    operators = (
        [source]
        + mappers_region1
        + mappers_region3
        + [
            reducer_a,
            reducer_b,
            reducer_c,
            global_reducer_a,
            global_reducer_b,
            global_reducer_c,
        ]
    )

    return operators, source


if __name__ == "__main__":
    operators, source = create_hierarchical_operators()

    scheduler = AdaptiveScheduler(operators, source, TOTAL_MEMORY_CAPACITY)
    scheduler.run()
