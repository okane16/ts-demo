import {
  ClickHouseInt,
  Key,
  OlapTable,
  Task,
  Workflow,
  ClickHouseEngines,
  MaterializedView,
  sql,
} from "@514labs/moose-lib";
import { faker } from "@faker-js/faker";

// Define a data model for the table
interface Original {
  id: Key<string>;
  a_number: number;
  a_string: string;
  a_date: Date;
}

const table = new OlapTable<Original>("new_table");

interface Target {
  day: Date;
  count: number;
  sum: number;
  avg: number;
}

const targetTable = new OlapTable<Target>("renamed", {
  engine: ClickHouseEngines.MergeTree,
  orderByFields: ["day"],
});

const select = sql`
  SELECT 
    toStartOfDay(${table.columns.a_date}) as day, 
    uniq(${table.columns.id}) as count, 
    SUM(${table.columns.a_number}) as sum,
    avg(${table.columns.a_number}) as avg
  FROM ${table} 
  GROUP BY day`;

const mv = new MaterializedView<Target>({
  selectStatement: select,
  selectTables: [table],
  targetTable: targetTable,
  orderByFields: ["day"],
  materializedViewName: "mv_to_target",
});

// Define a task to seed the table with random data
const seed = new Task<null, void>("seed", {
  run: async () => {
    for (let i = 0; i < 1000; i++) {
      table.insert([
        {
          id: faker.string.uuid(),
          a_number: faker.number.int({ min: 18, max: 100 }),
          a_string: faker.string.uuid(),
          a_date: faker.date.recent(),
        },
      ]);
    }
  },
});

const workflow = new Workflow("seed", {
  startingTask: seed,
});
