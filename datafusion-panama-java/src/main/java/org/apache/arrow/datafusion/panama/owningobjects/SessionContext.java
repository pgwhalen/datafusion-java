package org.apache.arrow.datafusion.panama.owningobjects;

import org.apache.arrow.datafusion.panama.DataFusionFFI;
import org.apache.arrow.vector.ipc.ArrowReader;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;

public class SessionContext implements AutoCloseable {

    private final MemorySegment pointer;
    private final Arena arena;

    static SessionContext make() {
        var sessionPtr = DataFusionFFI.sessionContextNew();
        if (sessionPtr.equals(MemorySegment.NULL)) {
            throw new RuntimeException("Failed to make session context");
        }
        var arena = Arena.ofShared();
        return new SessionContext(sessionPtr, arena);
    }

    private SessionContext(MemorySegment pointer, Arena arena) {
        this.pointer = pointer;
        this.arena = arena;
    }

    ArrowReader runSql(String sql) {
        var sqlString = arena.allocateUtf8String(sql);
//        DataFusionFFI.sessionContextSqlArrowStream()
        return null;
    }

    @Override
    public void close() throws Exception {
        DataFusionFFI.sessionContextDestroy(pointer);
        arena.close();
    }
}
