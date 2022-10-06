// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

import { Skeleton, Box } from "@chakra-ui/react";
import { Column, DataTable } from "./DataTable";

export enum StageStatus {
  QUEUED = "QUEUED",
  RUNNING = "RUNNING",
  FAILED = "FAILED",
  COMPLETED = "COMPLETED",
}

export interface Stage {
  stage_id: string;
  stage_status: StageStatus;
  input_rows: number;
  output_rows: number;
  elapsed_compute: string;
}

export interface StagesListProps {
  stages?: Stage[];
}

const columns: Column<any>[] = [
  {
    Header: "Stage ID",
    accessor: "stage_id",
  },
  {
    Header: "Status",
    accessor: "stage_status",
  },
  {
    Header: "Input Rows",
    accessor: "input_rows",
  },
  {
    Header: "Output Rows",
    accessor: "output_rows",
  },
  {
    Header: "Computation time",
    accessor: "elapsed_compute",
  },
];

const getSkeleton = () => (
  <>
    <Skeleton height={5} />
    <Skeleton height={5} />
    <Skeleton height={5} />
    <Skeleton height={5} />
    <Skeleton height={5} />
  </>
);

export const JobStagesQueries: React.FunctionComponent<StagesListProps> = ({
  stages,
}) => {
  const isLoaded = typeof stages !== "undefined";

  return (
    <Box w={"100%"} flex={1}>
      {isLoaded ? (
        <DataTable
          columns={columns}
          data={stages || []}
          pageSize={10}
          pb={10}
        />
      ) : (
        getSkeleton()
      )}
    </Box>
  );
};
