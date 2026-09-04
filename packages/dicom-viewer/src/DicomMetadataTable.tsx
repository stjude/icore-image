/**
 * This DICOM metadata table implementation was heavily pulled from the
 * official dicom-parser example, but ported to typescript, streamlined,
 * and adapted to React.
 *
 * See https://github.com/cornerstonejs/dicomParser/blob/master/examples/dumpWithDataDictionary/index.html
 */
import { DataSet } from 'dicom-parser';
import { Table } from '@mantine/core';
import { TAG_DICT, DataDictEntry, uids } from './DicomDataDictionary';

interface DicomMetadataTableRow {
    tag: string;
    vr: string;
    valueString?: string;
    length?: number;
    indent: number;
    dataOffset?: number;
    sequencePosition?: number;
}

const isStringVr = (vr?: DataDictEntry['vr']) => {
    return !(
        vr === 'AT' ||
        vr === 'FL' ||
        vr === 'FD' ||
        vr === 'OB' ||
        vr === 'OF' ||
        vr === 'OW' ||
        vr === 'SI' ||
        vr === 'SQ' ||
        vr === 'SS' ||
        vr === 'SL' ||
        vr === 'UL' ||
        vr === 'UT' ||
        vr === 'UN' ||
        vr === 'US'
    );
};

// helper function to see if a string only has ascii characters in it
const isASCII = (str: string) => {
    return /^[\x00-\x7F]*$/.test(str);
};

const mapUid = (str: string) => {
    const uid = uids[str];
    if (uid) {
        return ' [ ' + uid + ' ]';
    }
    return '';
};

const parseValueForImplicitVR = (ds: DataSet, element: DataSet['elements'][string]): string => {
    const propertyName = element.tag;
    let text = '';
    // First we check to see if the element's length is appropriate for a UI or US VR.
    // US is an important type because it is used for the
    // image Rows and Columns so that is why those are assumed over other VR types.
    if (element.length === 2) {
        text += ' (' + ds.uint16(propertyName) + ')';
    } else if (element.length === 4) {
        text += ' (' + ds.uint32(propertyName) + ')';
    }

    // Next we ask the dataset to give us the element's data in string form.  Most elements are
    // strings but some aren't so we do a quick check to make sure it actually has all ascii
    // characters so we know it is reasonable to display it.
    const str = ds.string(propertyName);
    const stringIsAscii = str ? isASCII(str) : false;

    if (stringIsAscii) {
        // the string will be undefined if the element is present but has no data
        // (i.e. attribute is of type 2 or 3 ) so we only display the string if it has
        // data.  Note that the length of the element will be 0 to indicate "no data"
        // so we don't put anything here for the value in that case.
        if (str !== undefined) {
            text += str + '"' + mapUid(str);
        }
    } else {
        if (element.length !== 2 && element.length !== 4) {
            // If it is some other length and we have no string
            text += 'binary data';
        }
    }
    return text;
};

const parseValueStringFromElement = (
    ds: DataSet,
    element: DataSet['elements'][string],
    tag?: DataDictEntry,
): string => {
    const maxLength = 128;
    // use VR to display the right value
    let vr: DataDictEntry['vr'] | undefined;
    if (element.vr !== undefined) {
        vr = element.vr as DataDictEntry['vr'];
    } else {
        if (tag !== undefined) {
            vr = tag.vr;
        }
    }

    const propertyName = element.tag;
    let text = '';
    // if the length of the element is less than 128 we try to show it.  We put this check in
    // to avoid displaying large strings which makes it harder to use.
    if (element.length < maxLength) {
        if (element.vr === undefined && tag === undefined) {
            return parseValueForImplicitVR(ds, element);
        } else {
            if (isStringVr(vr)) {
                // Next we ask the dataset to give us the element's data in string form.  Most elements are
                // strings but some aren't so we do a quick check to make sure it actually has all ascii
                // characters so we know it is reasonable to display it.
                const str = ds.string(propertyName);
                const stringIsAscii = str ? isASCII(str) : false;

                if (stringIsAscii) {
                    // the string will be undefined if the element is present but has no data
                    // (i.e. attribute is of type 2 or 3 ) so we only display the string if it has
                    // data.  Note that the length of the element will be 0 to indicate "no data"
                    // so we don't put anything here for the value in that case.
                    if (str !== undefined) {
                        text += str + mapUid(str);
                    }
                } else if (element.length === 0) {
                    text += '<Empty>';
                } else {
                    if (element.length !== 2 && element.length !== 4) {
                        // If it is some other length and we have no string
                        text += 'binary data';
                    }
                }
            } else if (vr === 'US') {
                text += ds.uint16(propertyName);
                for (let i = 1; i < ds.elements[propertyName].length / 2; i++) {
                    text += '\\' + ds.uint16(propertyName, i);
                }
            } else if (vr === 'SS') {
                text += ds.int16(propertyName);
                for (let i = 1; i < ds.elements[propertyName].length / 2; i++) {
                    text += '\\' + ds.int16(propertyName, i);
                }
            } else if (vr === 'UL') {
                text += ds.uint32(propertyName);
                for (let i = 1; i < ds.elements[propertyName].length / 4; i++) {
                    text += '\\' + ds.uint32(propertyName, i);
                }
            } else if (vr === 'SL') {
                text += ds.int32(propertyName);
                for (let i = 1; i < ds.elements[propertyName].length / 4; i++) {
                    text += '\\' + ds.int32(propertyName, i);
                }
            } else if (vr == 'FD') {
                text += ds.double(propertyName);
                for (let i = 1; i < ds.elements[propertyName].length / 8; i++) {
                    text += '\\' + ds.double(propertyName, i);
                }
            } else if (vr == 'FL') {
                text += ds.float(propertyName);
                for (let i = 1; i < ds.elements[propertyName].length / 4; i++) {
                    text += '\\' + ds.float(propertyName, i);
                }
            } else if (vr === 'OB' || vr === 'OW' || vr === 'UN' || vr === 'OF' || vr === 'UT') {
                // If it is some other length and we have no string
                if (element.length === 2) {
                    text += 'binary data' + ' of length ' + element.length + ' as uint16: ' + ds.uint16(propertyName);
                } else if (element.length === 4) {
                    text += 'binary data' + ' of length ' + element.length + ' as uint32: ' + ds.uint32(propertyName);
                } else {
                    text += 'binary data' + ' of length ' + element.length + ' and VR ' + vr;
                }
            } else if (vr === 'AT') {
                const group = ds.uint16(propertyName, 0);
                const groupHexStr = ('0000' + group?.toString(16) || '').slice(-4);
                const element = ds.uint16(propertyName, 1);
                const elementHexStr = ('0000' + element?.toString(16) || '').slice(-4);
                text += 'x' + groupHexStr + elementHexStr;
            } else if (vr === 'SQ') {
            } else {
                // If it is some other length and we have no string
                text += 'No display code for VR ' + vr;
            }
        }
    } else {
        // Add text saying the data is too long to show...
        text = 'data of length ' + element.length + ' for VR ' + vr + ' too long to show';
    }
    return text;
};

const getTag = (tag: string): DataDictEntry | undefined => {
    const group = tag.substring(1, 5);
    const element = tag.substring(5, 9);
    const tagIndex = ('(' + group + ',' + element + ')').toUpperCase();
    return TAG_DICT[tagIndex];
};

const dumpDataSet = (
    ds: DataSet,
    indent: number = 0,
    sequencePosition: number | undefined = undefined,
): DicomMetadataTableRow[] => {
    const keys = Object.keys(ds.elements).sort();
    const rows: DicomMetadataTableRow[] = [];
    for (const key of keys) {
        const element = ds.elements[key];
        const tag = getTag(key);
        const row: DicomMetadataTableRow = {
            tag: tag?.name || key,
            vr: element.vr || 'UN',
            length: element.length,
            dataOffset: element.dataOffset,
            indent,
            sequencePosition,
        };
        let valueString;
        const childRows: DicomMetadataTableRow[] = [];
        // If the element is a sequence, recursively dump its items
        if (element.vr === 'SQ' && element.items) {
            valueString = 'Sequence with ' + element.items.length + ' items';
            element.items.forEach((item, index) => {
                if (!item.dataSet) {
                    console.warn(`Item ${index} in sequence at tag ${key} has no dataSet`);
                    return;
                }
                childRows.push(...dumpDataSet(item.dataSet, indent + 1, index));
            });
        } else if (element.fragments) {
            valueString = `Encapsulated pixel data with ${element.basicOffsetTable?.length} offsets and ${element.fragments.length} fragments`;
            // TODO: Need to do deeper fragment handling?
        } else {
            valueString = parseValueStringFromElement(ds, element, tag);
        }
        row.valueString = valueString;
        rows.push(row, ...childRows);
    }
    return rows;
};

const DicomMetadataTable = ({ ds, visible }: { ds: DataSet; visible: boolean }) => {
    if (!visible) {
        return null;
    }

    const tableRows = dumpDataSet(ds);

    return (
        <Table.ScrollContainer minWidth={400} style={{ borderRadius: 8, border: '1px solid var(--mantine-color-gray-3)' }}>
            <Table stickyHeader highlightOnHover fz="sm">
                <Table.Thead bg="gray.0">
                    <Table.Tr>
                        <Table.Th>Tag</Table.Th>
                        <Table.Th>Value</Table.Th>
                    </Table.Tr>
                </Table.Thead>
                <Table.Tbody>
                    {tableRows.map((row, index) => (
                        <Table.Tr key={index}>
                            <Table.Td ff="monospace" fz="xs" pl={16 + row.indent * 20}>
                                {row.tag}
                            </Table.Td>
                            <Table.Td c="dimmed" style={{ wordBreak: 'break-all' }}>
                                {row.valueString || ''}
                            </Table.Td>
                        </Table.Tr>
                    ))}
                </Table.Tbody>
            </Table>
        </Table.ScrollContainer>
    );
};
export default DicomMetadataTable;
