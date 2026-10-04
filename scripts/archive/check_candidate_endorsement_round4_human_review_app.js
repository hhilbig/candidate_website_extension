async page => {
  const consoleErrors = [];
  const pageErrors = [];
  page.on("console", message => {
    if (message.type() === "error") consoleErrors.push(message.text());
  });
  page.on("pageerror", error => pageErrors.push(error.message));

  await page.evaluate(() => localStorage.clear());
  await page.reload();
  const initial = await page.evaluate(() => {
    const api = window.__round4ReviewApp;
    const state = api.getState();
    const responses = Object.values(state);
    const children = responses.flatMap(response => response.children);
    let exportBlocked = false;
    try {
      api.buildExport();
    } catch (error) {
      exportBlocked = /not fully and explicitly confirmed/.test(error.message);
    }
    return {
      parentCount: T.parents.length,
      stateParentCount: responses.length,
      allParentsUnconfirmed: responses.every(
        response => response.parent_confirmed === false,
      ),
      allChildrenUnconfirmed: children.every(child => child.confirmed === false),
      banner: document.getElementById("reviewBanner").textContent,
      progress: document.getElementById("progress").textContent,
      taskAuthorityFlags: T.authorizations,
      resultAuthorityFlags: A.result_authorizations,
      exportBlocked,
    };
  });
  if (
    initial.parentCount !== 40
    || initial.stateParentCount !== 40
    || !initial.allParentsUnconfirmed
    || !initial.allChildrenUnconfirmed
    || initial.banner !== "AI first cut—human review pending"
    || !initial.progress.includes("0/40 complete")
    || Object.values(initial.taskAuthorityFlags).some(value => value !== false)
    || initial.resultAuthorityFlags.human_review_complete !== true
    || ['development', 'human_truth', 'production_integration', 'full_expansion', 'release']
      .some(field => initial.resultAuthorityFlags[field] !== false)
    || !initial.exportBlocked
  ) throw Error(`initial review gate failed: ${JSON.stringify(initial)}`);

  const selectionSetup = await page.evaluate(() => {
    parentIndex = T.parents.findIndex(parent => parent.ai_children.length > 0);
    if (parentIndex < 0) throw Error("browser fixture has no provisional child");
    childIndex = 0;
    const parent = curParent();
    const response = S[parent.parent_document_id];
    const expected = [...response.children[0].name_spans[0]];
    response.children[0].name_spans = [];
    response.children[0].confirmed = false;
    draw();
    const node = document.getElementById("source").firstChild;
    const text = parent.parent_source_text;
    const codepoints = Array.from(text);
    const startUtf16 = codepoints.slice(0, expected[0]).join("").length;
    const endUtf16 = codepoints.slice(0, expected[1]).join("").length;
    const range = document.createRange();
    range.setStart(node, startUtf16);
    range.setEnd(node, endUtf16);
    const selection = window.getSelection();
    selection.removeAllRanges();
    selection.addRange(range);
    document.getElementById("source").focus();
    rememberSelection();
    return { parentId: parent.parent_document_id, expected, observed: selectionSpan() };
  });
  if (JSON.stringify(selectionSetup.expected) !== JSON.stringify(selectionSetup.observed)) {
    throw Error(`Chrome code-point selection failed: ${JSON.stringify(selectionSetup)}`);
  }
  await page.locator("#addName").click();
  const selectionPersisted = await page.evaluate(() => {
    const response = S[curParent().parent_document_id];
    return {
      nameSpans: response.children[0].name_spans,
      autosaved: localStorage.getItem(storageKey) !== null,
    };
  });
  if (
    JSON.stringify(selectionPersisted.nameSpans) !== JSON.stringify([
      selectionSetup.expected,
    ])
    || !selectionPersisted.autosaved
  ) throw Error(`selection persistence/autosave failed: ${JSON.stringify(selectionPersisted)}`);

  await page.evaluate(() => document.getElementById('nonzero').focus());
  await page.keyboard.press("c");
  await page.keyboard.press("1");
  await page.keyboard.press("Enter");
  const keyboard = await page.evaluate(parentId => {
    const response = S[parentId];
    return {
      parentConfirmed: response.parent_confirmed,
      type: response.children[0].entity_type,
      childConfirmed: response.children[0].confirmed,
    };
  }, selectionSetup.parentId);
  if (
    !keyboard.parentConfirmed
    || keyboard.type !== "individual"
    || !keyboard.childConfirmed
  ) throw Error(`keyboard confirmation failed: ${JSON.stringify(keyboard)}`);

  const beforeSplitCount = await page.evaluate(parentId => {
    parentIndex = T.parents.findIndex(parent => parent.parent_document_id === parentId);
    childIndex = 0;
    const response = S[parentId];
    response.children[0].confirmed = true;
    draw();
    document.getElementById('nonzero').focus();
    return response.children.length;
  }, selectionSetup.parentId);
  await page.keyboard.press("s");
  const split = await page.evaluate(() => {
    const children = S[curParent().parent_document_id].children;
    return {
      count: children.length,
      copiedSpans:
        JSON.stringify(children[0].name_spans) === JSON.stringify(children[1].name_spans),
      bothUnconfirmed: !children[0].confirmed && !children[1].confirmed,
    };
  });
  if (split.count !== beforeSplitCount + 1 || !split.copiedSpans || !split.bothUnconfirmed) {
    throw Error(`split workflow failed: ${JSON.stringify(split)}`);
  }

  const preComplete = await page.evaluate(() => {
    for (const parent of T.parents) {
      const response = seedState(parent);
      response.parent_confirmed = true;
      response.reviewed_unresolved_block_ids = parent.ai_unresolved_blocks.map(
        block => block.unresolved_block_id,
      );
      if (response.children.length) {
        response.true_zero = false;
        for (const child of response.children) {
          if (!['individual', 'organization'].includes(child.entity_type)) {
            child.entity_type = 'individual';
          }
          if (!child.support_spans.length) child.support_spans = cloneSpans(child.name_spans);
          child.confirmed = true;
        }
      } else {
        response.true_zero = true;
      }
      S[parent.parent_document_id] = response;
    }
    const unresolvedParentIndex = T.parents.findIndex(
      parent => parent.ai_unresolved_blocks.length > 0,
    );
    let unresolved = null;
    if (unresolvedParentIndex >= 0) {
      parentIndex = unresolvedParentIndex;
      childIndex = 0;
      const parent = T.parents[parentIndex];
      const block = parent.ai_unresolved_blocks[0];
      const response = S[parent.parent_document_id];
      response.reviewed_unresolved_block_ids = response.reviewed_unresolved_block_ids.filter(
        id => id !== block.unresolved_block_id,
      );
      unresolved = {
        parentId: parent.parent_document_id,
        blockId: block.unresolved_block_id,
        span: block.block_span,
        blockedBeforeReview: !parentComplete(parent, response),
      };
    }
    save();
    draw();
    document.getElementById('nonzero').focus();
    return unresolved;
  });
  let blockWorkflow = { exercised: false };
  if (preComplete) {
    await page.keyboard.press("u");
    const shown = await page.evaluate(() => ({
      activeSource: document.activeElement === document.getElementById("source"),
      selected: selectionSpan(),
    }));
    await page.keyboard.press("r");
    blockWorkflow = await page.evaluate(expected => {
      const parent = T.parents.find(value => value.parent_document_id === expected.parentId);
      const response = S[expected.parentId];
      return {
        exercised: true,
        blockedBeforeReview: expected.blockedBeforeReview,
        shown: expected.shown,
        reviewedAfterR: response.reviewed_unresolved_block_ids.includes(expected.blockId),
        completeAfterR: parentComplete(parent, response),
      };
    }, { ...preComplete, shown });
    if (
      !blockWorkflow.blockedBeforeReview
      || !blockWorkflow.shown.activeSource
      || JSON.stringify(blockWorkflow.shown.selected) !== JSON.stringify(preComplete.span)
      || !blockWorkflow.reviewedAfterR
      || !blockWorkflow.completeAfterR
    ) throw Error(`failed-slot keyboard review failed: ${JSON.stringify(blockWorkflow)}`);
  }

  const completed = await page.evaluate(() => {
    const exported = buildExport();
    const imported = validateImported(exported);
    return {
      parentCount: exported.parent_count,
      reviewStatus: exported.review_status,
      parentFlags: exported.responses.every(response => response.parent_confirmed),
      childFlags: exported.responses.every(response => (
        response.children.every(child => child.human_confirmed)
      )),
      blockFlags: exported.responses.every((response, index) => (
        JSON.stringify(response.reviewed_unresolved_block_ids) === JSON.stringify(
          T.parents[index].ai_unresolved_blocks.map(block => block.unresolved_block_id),
        )
      )),
      expansion: exported.authorizations.full_expansion,
      release: exported.authorizations.release,
      authorityFlags: exported.authorizations,
      importParentCount: Object.keys(imported).length,
    };
  });
  if (
    completed.parentCount !== 40
    || completed.reviewStatus !== "all_parents_children_and_unresolved_blocks_explicitly_confirmed"
    || !completed.parentFlags
    || !completed.childFlags
    || !completed.blockFlags
    || completed.expansion !== false
    || completed.release !== false
    || completed.authorityFlags.human_review_complete !== true
    || ['development', 'human_truth', 'production_integration', 'full_expansion', 'release']
      .some(field => completed.authorityFlags[field] !== false)
    || completed.importParentCount !== 40
  ) throw Error(`complete export/import failed: ${JSON.stringify(completed)}`);

  if (consoleErrors.length || pageErrors.length) {
    throw Error(JSON.stringify({ consoleErrors, pageErrors }));
  }
  return {
    status: "PASS",
    initial,
    selectionSetup,
    selectionPersisted,
    keyboard,
    split,
    blockWorkflow,
    completed,
    consoleErrorCount: consoleErrors.length,
    pageErrorCount: pageErrors.length,
  };
}
