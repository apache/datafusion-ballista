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

import React, { useEffect, useState } from "react";
import { ExternalLinkIcon } from "@chakra-ui/icons";
import {
  CircularProgress,
  CircularProgressLabel,
  Skeleton,
  Flex,
  Box,
  useDisclosure,
  Button,
  Modal,
  ModalBody,
  ModalCloseButton,
  ModalContent,
  ModalFooter,
  ModalHeader,
  ModalOverlay,
  Link,
} from "@chakra-ui/react";
import { Column, DataTable } from "./DataTable";
import { FaStop } from "react-icons/fa";
import { GrDocumentDownload, GrOverview } from "react-icons/gr";
import fileDownload from "js-file-download";
import SVG from "react-inlinesvg";
import { JobStagesQueries } from "./JobStagesMetrics";

export enum QueryStatus {
  QUEUED = "QUEUED",
  RUNNING = "RUNNING",
  FAILED = "FAILED",
  COMPLETED = "COMPLETED",
}

export interface Query {
  job_id: string;
  job_name: string;
  status: QueryStatus;
  num_stages: number;
  percent_complete: number;
}

export interface QueriesListProps {
  queries?: Query[];
}

export const ActionsCell: (props: any) => React.ReactNode = (props: any) => {
  const [dot_data, setData] = useState("");
  const { isOpen, onOpen, onClose } = useDisclosure();
  const ref = React.useRef<SVGElement>(null);

  const dot_svg = (url: string) => {
    fetch(url, {
      method: "GET",
      headers: {
        Accept: "application/json",
      },
    }).then(async (res) => {
      setData(await res.text());
    });
  };

  useEffect(() => {
    if (isOpen) {
      dot_svg("/api/job/" + props.value + "/dot_svg");
    }
  }, [ref.current, dot_data, isOpen]);

  const handleDownload = (url: string, filename: string) => {
    fetch(url, {
      method: "GET",
      headers: {
        Accept: "application/json",
      },
    }).then(async (res) => {
      fileDownload(await res.arrayBuffer(), filename);
    });
  };
  return (
    <Flex>
      <FaStop color={"red"} title={"Stop this job"} />
      <Box mx={2}></Box>
      <button
        onClick={() => {
          handleDownload(
            "/api/job/" + props.value + "/dot",
            props.value + ".dot"
          );
        }}
      >
        <GrDocumentDownload title={"Download DOT Plan"} />
      </button>
      <Box mx={2}></Box>
      <button onClick={onOpen}>
        <GrOverview title={"View Graph"} />
      </button>
      <Modal isOpen={isOpen} size="small" onClose={onClose}>
        <ModalOverlay />
        <ModalContent>
          <ModalHeader textAlign={"center"}>
            Graph for {props.value} job
          </ModalHeader>
          <ModalCloseButton />
          <ModalBody margin="auto">
            <SVG innerRef={ref} src={dot_data} width="auto" />
          </ModalBody>
          <ModalFooter>
            <Button colorScheme="blue" mr={3} onClick={onClose}>
              Close
            </Button>
          </ModalFooter>
        </ModalContent>
      </Modal>
    </Flex>
  );
};

export const JobLinkCell: (props: any) => React.ReactNode = (props: any) => {
  const [stages, setData] = useState();
  const [loaded, setLoaded] = useState(false);
  const { isOpen, onOpen, onClose } = useDisclosure();

  const getStages = (url: string) => {
    fetch(url, {
      method: "GET",
      headers: {
        Accept: "application/json",
      },
    }).then(async (res) => {
      const jsonObj = await res.json();
      setData(jsonObj["stages"]);
    });
  };

  useEffect(() => {
    if (isOpen && !loaded) {
      getStages("/api/job/" + props.value + "/stages");
      setLoaded(true);
    }
  }, [stages, isOpen]);

  return (
    <Flex>
      <Link onClick={onOpen} icon>
        {props.value} <ExternalLinkIcon mx="2px" />
      </Link>
      <Modal isOpen={isOpen} size="small" onClose={onClose}>
        <ModalOverlay />
        <ModalContent>
          <ModalHeader textAlign={"center"}>
            Stages metrics for {props.value} job
          </ModalHeader>
          <ModalCloseButton />
          <ModalBody margin="auto">
            <JobStagesQueries stages={stages} />
          </ModalBody>
          <ModalFooter>
            <Button colorScheme="blue" mr={3} onClick={onClose}>
              Close
            </Button>
          </ModalFooter>
        </ModalContent>
      </Modal>
    </Flex>
  );
};

export const ProgressCell: (props: any) => React.ReactNode = (props: any) => {
  return (
    <CircularProgress value={props.value} color="orange.400">
      <CircularProgressLabel>{props.value}%</CircularProgressLabel>
    </CircularProgress>
  );
};

const columns: Column<any>[] = [
  {
    Header: "Job ID",
    accessor: "job_id",
    Cell: JobLinkCell,
  },
  {
    Header: "Job Name",
    accessor: "job_name",
  },
  {
    Header: "Status",
    accessor: "job_status",
  },
  {
    Header: "Number of Stages",
    accessor: "num_stages",
  },
  {
    Header: "Progress",
    accessor: "percent_complete",
    Cell: ProgressCell,
  },
  {
    Header: "Actions",
    accessor: "job_id",
    id: "action_cell",
    Cell: ActionsCell,
  },
];

const getSkeleton = () => (
  <>
    <Skeleton height={5} />
    <Skeleton height={5} />
    <Skeleton height={5} />
    <Skeleton height={5} />
    <Skeleton height={5} />
    <Skeleton height={5} />
  </>
);

export const QueriesList: React.FunctionComponent<QueriesListProps> = ({
  queries,
}) => {
  const isLoaded = typeof queries !== "undefined";

  return (
    <Box w={"100%"} flex={1}>
      {isLoaded ? (
        <DataTable
          columns={columns}
          data={queries || []}
          pageSize={10}
          pb={10}
        />
      ) : (
        getSkeleton()
      )}
    </Box>
  );
};
