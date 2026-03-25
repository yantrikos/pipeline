#!/usr/bin/env python3
"""Pipeline Bridge — JSON stdin/stdout for TypeScript plugin."""

import sys
import json
import signal

from pipeline_engine.engine import PipelineEngine

_engine: PipelineEngine = None


def get_engine(config: dict = None) -> PipelineEngine:
    global _engine
    if _engine is None:
        _engine = PipelineEngine(config)
    return _engine


def handle_command(command: str, args: dict, config: dict) -> dict:
    engine = get_engine(config)
    try:
        if command == "register_workflow":
            wid = engine.register_workflow(args.get("definition", {}))
            return {"success": True, "workflow_id": wid}
        elif command == "start_execution":
            eid = engine.start_execution(
                args["workflow_id"], inputs=args.get("inputs"),
                trigger_type=args.get("trigger_type", "manual"))
            return {"success": True, "execution_id": eid}
        elif command == "get_execution":
            return {"success": True, "execution": engine.get_execution(args["execution_id"])}
        elif command == "list_executions":
            return {"success": True, "executions": engine.list_executions(
                workflow_id=args.get("workflow_id"), status=args.get("status"), limit=args.get("limit", 50))}
        elif command == "cancel_execution":
            return {"success": engine.cancel_execution(args["execution_id"])}
        elif command == "submit_approval":
            return {"success": engine.submit_approval(
                args["execution_id"], args["node_id"], args.get("approved", False),
                data=args.get("data"), actor=args.get("actor", ""))}
        elif command == "list_workflows":
            return {"success": True, "workflows": engine.list_workflows()}
        elif command == "get_pending_approvals":
            return {"success": True, "approvals": engine.get_pending_approvals()}
        elif command == "stats":
            return {"success": True, **engine.stats()}
        elif command == "health_check":
            return engine.health_check()
        elif command == "healing_history":
            return {"success": True, "history": engine.get_healing_history(
                execution_id=args.get("execution_id"), limit=args.get("limit", 50))}
        else:
            return {"success": False, "error": f"Unknown command: {command}"}
    except Exception as e:
        return {"success": False, "error": str(e), "code": getattr(e, "code", "PIPELINE_ERROR")}


def main():
    if "--persistent" in sys.argv:
        def shutdown(sig, frame):
            if _engine: _engine.close()
            sys.exit(0)
        signal.signal(signal.SIGTERM, shutdown)
        signal.signal(signal.SIGINT, shutdown)
        for line in sys.stdin:
            line = line.strip()
            if not line: continue
            try:
                data = json.loads(line)
                result = handle_command(data.get("command", ""), data.get("args", {}), data.get("config", {}))
                result["request_id"] = data.get("request_id")
                print(json.dumps(result), flush=True)
            except Exception as e:
                print(json.dumps({"success": False, "error": str(e)}), flush=True)
        if _engine: _engine.close()
    else:
        try:
            data = json.loads(sys.stdin.read())
        except json.JSONDecodeError:
            print(json.dumps({"success": False, "error": "Invalid JSON"}))
            return
        print(json.dumps(handle_command(data.get("command", ""), data.get("args", {}), data.get("config", {}))))


if __name__ == "__main__":
    main()
