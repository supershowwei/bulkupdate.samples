CREATE TYPE [dbo].[BulkTableType] AS TABLE
([Id]   [INT] NOT NULL,
 [Text] [NVARCHAR](MAX) NOT NULL,
 PRIMARY KEY([Id])
);