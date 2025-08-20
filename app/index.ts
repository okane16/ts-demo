import {
  ClickHouseInt,
  Key,
  OlapTable,
  Task,
  Workflow,
  ClickHouseEngines,
  MaterializedView,
  sql,
  ClickHouseDecimal,
} from "@514labs/moose-lib";
import { faker } from "@faker-js/faker";

// Define a data model for the table
interface Event {
  id: Key<string>;
  price: string & ClickHouseDecimal<10, 2>;
  color: string;
  created_at: Date;
}

const sourceTable = new OlapTable<Event>("source_table");

interface DailyRollup {
  day: Date;
  color: string;
  total_sales: number;
  avg_price: number;
}

const targetTable = new OlapTable<DailyRollup>("rollup_table", {
  engine: ClickHouseEngines.MergeTree,
  orderByFields: ["day"],
});

const select = sql`
  SELECT 
    toStartOfDay(${sourceTable.columns.created_at}) as day, 
    ${sourceTable.columns.color}, 
    sum(${sourceTable.columns.price}) as total_sales,
    avg(${sourceTable.columns.price}) as avg_price
  FROM ${sourceTable} 
  GROUP BY day, color`;

const mv = new MaterializedView<DailyRollup>({
  selectStatement: select,
  selectTables: [sourceTable],
  targetTable: targetTable,
  materializedViewName: "source_to_rollup_mv",
});

// Define a task to seed the table with random data
const seed = new Task<null, void>("seed", {
  run: async () => {
    for (let i = 0; i < 1000; i++) {
      sourceTable.insert([
        {
          id: faker.string.uuid(),
          price: faker.commerce.price({ min: 10, max: 100, dec: 2 }),
          color: faker.color.human(),
          created_at: faker.date.recent(),
        },
      ]);
    }
  },
});

const workflow = new Workflow("seed", {
  startingTask: seed,
});
