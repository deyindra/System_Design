package com.salesforce.einstein.hierarchy.api;

import com.salesforce.einstein.hierarchy.api.dto.Dtos.MoveJobResponse;
import com.salesforce.einstein.hierarchy.domain.Caller;
import com.salesforce.einstein.hierarchy.service.TreeService;
import org.springframework.lang.Nullable;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/** Progress of a large move accepted with 202. */
@RestController
@RequestMapping("/v1/move-jobs")
public class MoveJobController {
    private final TreeService tree;

    public MoveJobController(TreeService tree) {
        this.tree = tree;
    }

    @GetMapping("/{jobId}")
    public MoveJobResponse get(Caller caller, @PathVariable long jobId,
                               @RequestHeader(value = Http.TOKEN_HEADER, required = false) @Nullable String token) {
        return MoveJobResponse.of(tree.moveJob(caller, jobId, Http.token(token)));
    }
}
