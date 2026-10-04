"""Record bounded full-source AI judgments for 14 authenticated disagreements."""
from __future__ import annotations

import copy
import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from scripts import compare_candidate_endorsement_independent40_codex as comparison

review = comparison.review
PARENTS = (4, 7, 12, 14, 15, 16, 18, 20, 21, 27, 28, 31, 32, 39)
SEED_SHA256 = '54f3fb130bc1a61e5ec7e23d23303fbe13b0a25bafa0f85a9764b3c630cfb2fe'
REVIEWER_ID = 'independent_source_adjudicator'
REVIEWER_MODEL = 'gpt-5.6-sol'

# These are fixed judgments made after reading all 14 complete sources and both passes.
# They are not a rule for automatically adjudicating other parents.
DECISIONS = {
    4: [([[121, 407]], 'retain_blind_name_components', 'The expanded NRA-PVF name and its parenthetical acronym identify one organization. Retain both exact components without including parentheses.'),
        ([[2343, 2375], [4340, 4533], [4558, 4783]], 'retain_uncertain_occurrences', 'The damaged signature, generic committee, Smith surname, and college-group support remain source-spanned uncertain identities. Chairman is a role, not part of the surname.')],
    7: [([[1568, 1643]], 'retain_five_local_component_rows', 'Five local labels are explicitly named under the shared Ironworker name. Preserve five distinct source-component identities and the unresolved collective-versus-local multiplicity uncertainty; do not merge them into one accepted row.')],
    12: [([[2769, 2776], [5661, 5669], [17463, 19357]], 'retain_disjoint_occurrences', 'Ashford and Albrecht repeat at disjoint attribution coordinates. Union headings and named endorsement-letter labels repeat at disjoint coordinates; the approved occurrence rule retains each. Immediate parenthetical acronyms remain components of one occurrence.'),
         ([[14244, 14578], [14810, 15031]], 'exclude_rating_and_affiliation_only_seed_rows', 'National Right to Life, Susan B Anthony, and NRA in this passage issue ratings rather than endorsements. Nebraska Farm Bureau in the vice-president title identifies an affiliation. These seed-only coordinates are not additional endorsement occurrences; retain the source uncertainty without adding child rows.'),
         ([[18922, 19004]], 'retain_both_local_numbers_as_uncertain', 'The heading names Local 669 but its letter label names Local 699. Preserve both disjoint labels as uncertain; do not correct either source number.'),
         ([[19358, 19773]], 'retain_strong_public_support_as_uncertain', 'The source separates this group from its endorsement list. Preserve all eight identities with uncertain status rather than treating favorable public support as resolved electoral endorsement.')],
    14: [([[134, 232], [469, 508]], 'retain_moms_demand_action_uncertain', 'The page combines endorsements and distinctions without assigning the category. Keep the disputed Moms Demand Action row uncertain and retain both passes\' source uncertainty.')],
    15: [([[604, 1106], [1107, 7592], [8272, 8318]], 'retain_all_143_occurrences_as_date_uncertain', 'The page shows a 2015 post date and a 2022 footer, with titles reflecting different elections. Endorsement direction is established, but dates of the occurrences are unresolved. All 143 rows remain uncertain; no date exclusion is introduced.')],
    16: [([[284, 316], [317, 375]], 'retain_two_hatch_occurrences', 'The heading names Orrin Hatch and a later heading repeats his name at disjoint coordinates before the endorsement quote. The approved source-occurrence rule retains both.')],
    18: [([[186, 223], [896, 930], [1285, 1328], [1351, 1394]], 'retain_multiline_name_components', 'Each multiline organization name identifies one listed endorser. Keep all name components in one row for each organization. This resolves coordinate segmentation without changing identity multiplicity or status.')],
    20: [([[1776, 1962]], 'retain_two_uncertain_with_honor_occurrences', 'With Honor appears in the description and again in a standalone attribution at disjoint coordinates. Neither statement names Ferrara or establishes candidate-directed support. Retain both uncertain occurrences and both passes\' uncertainty blocks.')],
    21: [([[1160, 1200]], 'retain_multiline_organization_components', 'Interdenominational Ministerial Alliance is one organization across two lines in the endorsement list. Retain both components in one accepted occurrence, not two organizations.')],
    27: [([[2983, 3462]], 'retain_repeated_stubbins_occurrence', 'John Stubbins signs the endorsement and repeats in the following host attribution at disjoint coordinates. Preserve both personal-name occurrences. The show title is an affiliation, not an additional organization endorser.')],
    28: [([[199, 272]], 'use_surname_without_office_title', 'The listed person is Mack; Sheriff is an office title. Use the exact surname coordinates from the seed pass, with the complete listed attribution as support. The association is an affiliation rather than a separate organization endorsement.')],
    31: [([[54, 173], [1564, 1597], [3467, 3589]], 'retain_slate_choice_uncertainty', 'The slate says David Giles or Kelly Cooper. It does not unambiguously select Giles. Keep both disjoint John Birch Society mentions, the committee, and signatories uncertain; the other listed candidates are endorsement targets, not Giles endorsers.'),
         ([[3590, 4043]], 'retain_all_huckabee_occurrences_as_uncertain', 'Retain the disjoint heading, label, and signature occurrences. The quoted appeal supports conservative candidates generally in 2019/2020 without naming Giles. Neither the heading nor broad-party support resolves candidate direction or date. HuckPAC remains uncertain.'),
         ([[4044, 4184], [6064, 6249]], 'retain_omitted_disjoint_occurrences', 'The US IMPACT endorsement heading and the second narrative Jeff reference repeat names at disjoint coordinates in candidate-directed endorsement sections. Preserve these source occurrences rather than merging them.'),
         ([[4185, 4738]], 'use_biggs_surname_retain_speaker_uncertainty', 'Congressman is a role, so use the exact Biggs surname. The quote appears between the Biggs heading and Emma DiMarco attribution; both possible speakers remain uncertain.'),
         ([[6250, 7306]], 'retain_lesa_as_uncertain', 'Lesa signs the Patriot Movement AZ statement. The source does not establish a separate personal endorsement rather than signing for the organization.'),
         ([[7550, 7992]], 'retain_advanced_tactical_uncertain_hpr_accepted', 'Advanced Tactical Armament Concepts appears in the attribution, but the quote explicitly endorses on behalf of HPR Ammunition. Retain Advanced Tactical as uncertain and HPR as accepted. No separate organization endorsement is inferred from the affiliation.'),
         ([[4044, 11282]], 'retain_older_cycle_uncertainty_without_exclusion', 'District 4 and District 9 references and an explicit 2016 newspaper date leave current-cycle relevance unresolved. Preserve the original source-spanned date uncertainty; introduce no eligibility exclusion.')],
    32: [([[2575, 2622]], 'retain_unnamed_teamsters_locals_uncertain', 'The source names Joint Council 28 and unspecified Teamsters Locals. Retain the named council as accepted and the plural local label as uncertain; do not invent individual local identities.')],
    39: [([[3650, 3715]], 'remove_credential_from_exact_name', 'Karen McCormick is the personal name. DVM is a credential, not an additional name component. Use the seed name coordinates while retaining the complete list entry as support.'),
         ([[8420, 8503]], 'retain_alias_identity_uncertainty', 'Cavanaugh O\'Leary and Cav O\'Leary appear on distinct rows with different descriptions. Retain both source occurrences and the uncertainty about whether they identify one person.')],
}


def reviewed_judgment(index, blind, seed):
    """Apply only the fixed source judgments recorded above, preserving uncertainty."""
    children = copy.deepcopy(blind['children'])
    if index in {28, 39}:
        old_span = [[199, 211]] if index == 28 else [[3650, 3669]]
        new_span = [[207, 211]] if index == 28 else [[3650, 3665]]
        selected = next(row for row in seed['children'] if row['name_spans'] == new_span)
        children = [copy.deepcopy(selected) if row['name_spans'] == old_span else row for row in children]
    elif index == 27:
        children.append(copy.deepcopy(next(row for row in seed['children'] if row['name_spans'] == [[3449, 3462]])))
    elif index in {12, 31, 32}:
        uncertain_spans = ({((18922, 18955),), ((19408, 19448),), ((19451, 19494),),
                            ((19497, 19536),), ((19539, 19581),), ((19584, 19629),),
                            ((19632, 19671),), ((19674, 19718),), ((19721, 19771),)} if index == 12 else
                           {((7302, 7306),), ((7550, 7590),)} if index == 31 else {((2606, 2622),)})
        for row in children:
            if tuple(tuple(span) for span in row['name_spans']) in uncertain_spans:
                row['endorsement_status'] = 'uncertain'
        if index == 31:
            replacement = next(row for row in seed['children'] if row['name_spans'] == [[4197, 4202]])
            children = [copy.deepcopy(replacement) if row['name_spans'] == [[4185, 4202]] else row for row in children]
    uncertainties = []
    for item in blind['uncertainties'] + seed['uncertainties']:
        if item not in uncertainties:
            uncertainties.append(copy.deepcopy(item))
    return children, uncertainties


def record(source_dir=review.OUTPUT):
    source_dir = Path(source_dir)
    if (source_dir / 'adjudicated').exists() or (source_dir / 'adjudicated').is_symlink():
        raise ValueError('Adjudication requires a new directory; initial artifacts cannot be overwritten.')
    review.read_parent(1, source_dir)
    blind_path = source_dir / 'blind_reviewed_reference.json'
    seed_path = comparison.OUTPUT / 'seed_audit_reviewed_reference.json'
    if comparison.digest(blind_path.read_bytes()) != comparison.BLIND_SHA256 or comparison.digest(seed_path.read_bytes()) != SEED_SHA256:
        raise ValueError('Initial frozen AI reference hash differs.')
    parents = [comparison.strict_json(source_dir / 'source' / f'parent_{index:02d}.json') for index in range(1, 41)]
    blind = comparison.pass_reference('blind', parents, source_dir)
    seed = comparison.pass_reference('seed_audit', parents, source_dir)
    if blind != comparison.strict_json(blind_path) or seed != comparison.strict_json(seed_path):
        raise ValueError('Initial review inventory differs from its frozen reference.')
    receipts = []
    counts = {'parents': len(PARENTS), 'accepted_children': 0, 'uncertain_children': 0, 'uncertainty_blocks': 0}
    initial_hashes = {path: comparison.digest(path.read_bytes()) for path in source_dir.rglob('*.json')}
    for index in PARENTS:
        parent, a, b = parents[index - 1], blind['responses'][index - 1], seed['responses'][index - 1]
        children, uncertainties = reviewed_judgment(index, a, b)
        notes = 'Complete source and both initial AI passes inspected. ' + ' '.join(reason for _, _, reason in DECISIONS[index])
        value = review.write_review(index, children, REVIEWER_ID, pass_name='adjudicated',
                                    reviewer_model=REVIEWER_MODEL, complete_source_reviewed=True,
                                    uncertainties=uncertainties, notes=notes, output_dir=source_dir)
        comparison.reject_human_claims(value)
        counts['accepted_children'] += sum(row['endorsement_status'] == 'accepted_by_ai_review' for row in children)
        counts['uncertain_children'] += sum(row['endorsement_status'] == 'uncertain' for row in children)
        counts['uncertainty_blocks'] += len(uncertainties)
        receipts.append({'parent_index': index, 'parent_document_id': parent['parent_document_id'],
            'source_text_sha256': parent['source_text_sha256'], 'complete_source_reviewed': True,
            'initial_review_hashes': {pass_name: comparison.digest((source_dir / pass_name / f'parent_{index:02d}.json').read_bytes()) for pass_name in ('blind', 'seed_audit')},
            'adjudicated_review_sha256': comparison.digest((source_dir / 'adjudicated' / f'parent_{index:02d}.json').read_bytes()),
            'counts': {'accepted_children': sum(row['endorsement_status'] == 'accepted_by_ai_review' for row in children),
                       'uncertain_children': sum(row['endorsement_status'] == 'uncertain' for row in children), 'uncertainty_blocks': len(uncertainties)},
            'decisions': [{'source_spans': spans, 'decision': decision, 'reason': reason,
                           'evidence': comparison.span_evidence(parent, spans)} for spans, decision, reason in DECISIONS[index]],
            'uncertainty_decisions': [{'pass': pass_name, 'source_spans': copy.deepcopy(item['source_spans']),
                                      'reason': item['reason'], 'disposition': 'retain'}
                                     for pass_name, response in (('blind', a), ('seed_audit', b)) for item in response['uncertainties']],
            'human_reviewed': False})
    if any(comparison.digest(path.read_bytes()) != sha256 for path, sha256 in initial_hashes.items()):
        raise ValueError('An initial source-package artifact changed during adjudication.')
    receipt = {'version': 'independent40_bounded_source_ai_adjudication_receipt_v1',
        'reviewer_id': REVIEWER_ID, 'reviewer_model': REVIEWER_MODEL, 'parent_indices': list(PARENTS),
        'initial_blind_reference_sha256': comparison.BLIND_SHA256,
        'initial_seed_audit_reference_sha256': SEED_SHA256,
        'adjudication_script_sha256': comparison.digest(Path(__file__).read_bytes()),
        'counts': counts, 'parents': receipts, 'human_reviewed': False,
        'authorizations': copy.deepcopy(review.AUTHORIZATIONS),
        'interpretation': 'Bounded source adjudication by an AI reviewer, not human truth. All initial uncertainty is retained; exact duplicate blocks are stored once. No date exclusions or new coding definitions were introduced.'}
    review.utilities.write_json(source_dir / 'adjudicated/decision_receipt.json', receipt)
    return receipt


if __name__ == '__main__':
    result = record()
    print(json.dumps({'counts': result['counts'], 'receipt': str(review.OUTPUT / 'adjudicated/decision_receipt.json'), 'human_reviewed': False}, indent=2))
