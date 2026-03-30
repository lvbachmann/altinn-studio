using System;
using Microsoft.EntityFrameworkCore.Migrations;

#nullable disable

namespace WorkflowEngine.Data.Migrations
{
    /// <inheritdoc />
    public partial class RenameCorrelationIdToCollectionKey : Migration
    {
        /// <inheritdoc />
        protected override void Up(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.DropIndex(name: "IX_Workflows_CorrelationId", schema: "engine", table: "Workflows");

            migrationBuilder.DropColumn(name: "CorrelationId", schema: "engine", table: "Workflows");

            migrationBuilder.AddColumn<string>(
                name: "CollectionKey",
                schema: "engine",
                table: "Workflows",
                type: "character varying(200)",
                maxLength: 200,
                nullable: true
            );

            migrationBuilder.CreateIndex(
                name: "IX_Workflows_CollectionKey",
                schema: "engine",
                table: "Workflows",
                column: "CollectionKey"
            );
        }

        /// <inheritdoc />
        protected override void Down(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.DropIndex(name: "IX_Workflows_CollectionKey", schema: "engine", table: "Workflows");

            migrationBuilder.DropColumn(name: "CollectionKey", schema: "engine", table: "Workflows");

            migrationBuilder.AddColumn<Guid>(
                name: "CorrelationId",
                schema: "engine",
                table: "Workflows",
                type: "uuid",
                nullable: true
            );

            migrationBuilder.CreateIndex(
                name: "IX_Workflows_CorrelationId",
                schema: "engine",
                table: "Workflows",
                column: "CorrelationId"
            );
        }
    }
}
